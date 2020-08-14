"""
Update the report history collection

https://mesonet.agron.iastate.edu/request/download.phtml
"""

# stdlib
import asyncio as aio
from datetime import date, datetime, timedelta
from os import environ
from typing import Dict, List

# library
import httpx
from kewkew import Kew
from motor import MotorClient
from tqdm import tqdm
from avwx.station import station_list


END = date.today()
DAYS_TO_GO_BACK = 5


START = END - timedelta(days=DAYS_TO_GO_BACK)
SOURCE = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
    "station={}&data=metar&"
    "year1={}&month1={}&day1={}&"
    "year2={}&month2={}&day2={}&"
    "tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=null&"
    "trace=null&direct=no&report_type=1&report_type=2"
)


class Bar:
    """
    tqdm wrapper for workers to share
    """

    def set(self, total: int):
        self._bar = tqdm(total=total)

    def inc(self):
        self._bar.update(1)

    def close(self):
        self._bar.close()


BAR = Bar()


def find_timestamp(report: str) -> str:
    """
    Returns the Zulu timestamp without the trailing Z
    """
    for item in report.split():
        if len(item) == 7 and item.endswith("Z") and item[:6].isdigit():
            return item[:6]
    return None


def parse_response(text: str) -> Dict[str, dict]:
    """
    Returns valid reports from the raw response by date

    Ex: {"2020-01-01": {"011205": "KJFK 011205Z ...", ...}, ...}
    """
    lines = text.strip().split("\n")[1:]
    ret = {}
    for line in lines:
        line = line.split(",")
        date_key = line[1].split()[0]
        report = line[2].strip()
        # Source includes "null" lines and fake data
        # NOTE: https://mesonet.agron.iastate.edu/onsite/news.phtml?id=1290
        if not report or report == "null" or "MADISHF" in report:
            continue
        report_key = find_timestamp(report)
        if not report_key:
            continue
        try:
            ret[date_key][report_key] = report
        except KeyError:
            ret[date_key] = {report_key: report}
    return ret


class HistoryKew(Kew):
    """
    History process queue
    """

    def __init__(self, mdb: MotorClient, workers: int = 10):
        super().__init__(workers=workers)
        self.mdb = mdb

    async def worker(self, icao: str) -> bool:
        """
        Fetches and updates historical reports for an ICAO ident
        """
        BAR.inc()
        # Fetch records from NOAA
        url = SOURCE.format(
            icao, START.year, START.month, START.day, END.year, END.month, END.day,
        )
        try:
            async with httpx.AsyncClient() as conn:
                resp = await conn.get(url)
        except httpx.ReadTimeout:
            print(
                f"{icao}\t{START}\t{END}\t{datetime.utcnow()}",
                file=open("failed.tsv", "a"),
            )
            return False
        reports = parse_response(resp.text)
        # Update database
        for key, data in reports.items():
            key = datetime.strptime(key, r"%Y-%m-%d")
            find = {"icao": icao, "date": key}
            data = {**find, "raw": data}
            await self.mdb.history.metar.update_one(find, {"$set": data}, upsert=True)
        return True


def failed_stations() -> List[str]:
    """
    Returns the ICAO list of failed stations from the generated tsv file
    """
    with open("failed.tsv") as fin:
        stations = [l.split("\t")[0] for l in fin.readlines()]
    return stations


async def main():
    """
    Update the report history collection
    """
    mdb = MotorClient(environ["MONGO_URI"])
    kew = HistoryKew(mdb)
    stations = station_list()
    BAR.set(len(stations))
    for icao in stations:
        await kew.add(icao)
    await kew.finish()
    BAR.close()


if __name__ == "__main__":
    aio.run(main())
