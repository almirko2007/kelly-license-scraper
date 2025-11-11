from PySide6.QtCore import QThread, Signal,QEventLoop, QCoreApplication, QTimer
from playwright.async_api import async_playwright
from PySide6.QtWidgets import QApplication
import time
import asyncio
from Kelly_Scraper_Stelth_CTXT import DetailScraper
import re
import pandas as pd
from GoogleSheetsExporter import GoogleSheetsExporter
from Exporter_JSON_CSV_XLSX import Exporter
import os
from datetime import datetime

class Worker(DetailScraper):
    info = Signal(str)
    progress = Signal(int)
    results1 = Signal(list)
    results2 = Signal(list)
    finished = Signal()
    end = Signal(bool)

    def __init__(self):
        super().__init__()
   
        self.load_config()
        self.linkovi = []
        self.tab_linkovi = []
        self.tab_linkovi1 = []
        self.tabela1 = []
        self.tabela2 = []
        self.stop = False
        self.tasks = []

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        try:
            df = pd.read_json (config_path)
            self.keyword = df.loc[0, 'keyword']
            self.Url = df.loc[0, 'url']
            self.Headless = bool(df.loc[0, 'headless'])
            self.Semaphore = df.loc[0, 'semaphore']
        except Exception as e:
            self.error.emit("Nije upisan status scrapinga")
            self.keyword = "Atlanta"
            self.Url="https://www.kellysolutions.com/GA"
            self.Headless = False
            self.Semaphore = 10

    def Stop(self):
        print("Zaustavljanje programa, provjera 1.")
        self.stop = True
        if hasattr(self, "tasks"):
            for t in self.tasks:
                t.cancel()
        self.finished.emit()
    
    def scr_status(self, a):
        print("👉 scr_status() pozvana sa:", a)
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        try:
            df = pd.read_json(config_path)
            df["run"] = a  # Dodaje novu kolonu 'run' u svaki red
            df.to_json(config_path, orient="records", indent=4, force_ascii=False)
        except Exception as e:
            self.error.emit("Nije upisan status scrapinga")
            print("Nije upisan status scrapinga:", e)
    
    def save_progress (self, progres):
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        try:
            df = pd.read_json(config_path)
            df['progres'] = progres
            df.to_json(config_path, orient="records", indent=4, force_ascii=False)
        except Exception as e:
            self.LogInfo.setText("Nije uspio upis u config.json fajl")

    def run(self):
        print("▶️ run() metoda startuje")
        self.info.emit("Scraping...")
        self.scr_status(True)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.semaphore = asyncio.Semaphore(self.Semaphore)
        links1 = loop.run_until_complete(self.scrape_links1())
        links2 = loop.run_until_complete(self.scrape_detail1(links1))
        loop.run_until_complete(self.scrape_detail2(links2))
        loop.run_until_complete(self.scrape_detail3())
        loop.run_until_complete(self.scrape_table_detail1())
        loop.run_until_complete(self.export_Google_Sheet())
        loop.run_until_complete(self.export_File())
        self.results1.emit(self.tabela1)
        self.results2.emit(self.tabela2)
        if self.stop:
                self.error.emit("Scraping prekinut od starane korisnika")
                return
        self.info.emit("Scraping completed.")
        print("Scraping completed")
        self.scr_status(False)
        self.finished.emit()

    async def scrape_links1(self):
        try:
            urls=[self.Url]
            total = len(urls)
            async def wrapped_scrape(i, browser, url):
                print("URL: ", url)
                if self.stop:
                    self.results1.emit(self.tabela1)
                    self.results2.emit(self.tabela2)
                    print("Zaustavljanje programa, provjera 2.")
                    return []
                result = await self.scrape_links(browser, url)
                self.progress.emit(int((i + 1) / total * 20))
                self.save_progress(int((i + 1) / total * 20))
                self.info.emit(f"Link ->  {url}")
                return result
            async with async_playwright() as p:
            # Otvori browser jednom
                browser = await p.chromium.launch(headless=False)
                self.tasks = [
                    asyncio.create_task(wrapped_scrape(i, browser, url))
                    for i, url in enumerate(urls)
                ]
                links=await asyncio.gather(*self.tasks, return_exceptions=True)
                await browser.close()
                if links and len(links) > 0:
                     return links[0]
                else: 
                    return[]
        except:
            return[]

    async def scrape_detail1(self, urls):
        try:
            print("Startam 1....")
            linkovi = []
            tab_linkovi = []
            tabela = []
            total = len(urls)
            async def wrapped_scrape(i, browser, url):
                if self.stop:
                    self.results1.emit(self.tabela1)
                    self.results2.emit(self.tabela2)
                    print("Zaustavljanje programa, provjera 3.")
                    return []
                result = await self.scrape_detail(browser, url)
                self.progress.emit(20+int((i + 1) / total * 20))
                self.save_progress(20+int((i + 1) / total * 20))
                self.info.emit(f"Link ->  {url}")
                return result

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                self.tasks = [
                    asyncio.create_task(wrapped_scrape(i, browser, url))
                    for i, url in enumerate(urls)
                ]
                results=await asyncio.gather(*self.tasks, return_exceptions=True)
                await browser.close()
            if results and len(results) > 0:
                for res in results:
                    linkovi.extend(list(res[0]))       # prvi set
                    tab_linkovi.extend(list(res[1]))   # drugi set
                    tabela.extend(res[2]) 

                self.linkovi.extend(linkovi)
                self.tab_linkovi.extend(tab_linkovi)   
                self.tabela1.extend(tabela)
                for link in linkovi:
                    print("LL: ", link)
                return linkovi
            else: 
                return[]
        except: 
            return[]

    async def scrape_detail2(self, urls):
        try:
            print("Startam 2....")
            linkovi = []
            tab_linkovi = []
            tabela = []
            fil = [
            link for link in urls[:7]
            if not re.search(r"(searchbyCity|applynow|update|index|searchbyLicense|index|searchbyconame|registration)", link, re.IGNORECASE)
                ]  
            total = len(fil)
            async def wrapped_scrape(i, browser, url):
                if self.stop:
                    self.results1.emit(self.tabela1)
                    self.results2.emit(self.tabela2)
                    print("Zaustavljanje programa, provjera 4.")
                    return []
                result = await self.scrape_detail(browser, url)
                self.progress.emit(40+int((i + 1) / total * 20))
                self.save_progress(40+int((i + 1) / total * 20))
                self.info.emit(f"Link ->  {url}")
                return result

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                self.tasks = [
                    asyncio.create_task(wrapped_scrape(i, browser, url))
                    for i, url in enumerate(fil)
                ]
                results=await asyncio.gather(*self.tasks, return_exceptions=True)
                await browser.close()
            
            if results and len(results) > 0:
                for res in results:
                        linkovi.extend(list(res[0]))       # prvi set
                        tab_linkovi.extend(list(res[1]))   # drugi set
                        tabela.extend(res[2]) 

                self.linkovi.extend(linkovi)
                self.tab_linkovi.extend(tab_linkovi)   
                self.tabela1.extend(tabela)
                for link in linkovi:
                    print("LL: ", link)
        except:
            return
    async def scrape_detail3(self):
        try:
            print("Startam 3....")
            linkovi = []
            tab_linkovi = []
            tab_linkovi1 = []
            tabela = []
            fil = [
                    link for link in self.linkovi[:7]
                    if re.search(r"(searchbyCity)", link, re.IGNORECASE)
                        ]
            total = len(fil)
            print("Broj linkova za pretragu: ", len(fil))
            for link in fil:
                print("Link -> ", link)
            async def wrapped_scrape(i, browser, url):
                if self.stop:
                    self.results1.emit(self.tabela1)
                    self.results2.emit(self.tabela2)
                    print("Zaustavljanje programa, provjera 5.")
                    return []
                result = await self.scrape_detail(browser, url)
                self.progress.emit(60+int((i + 1) / total * 20))
                self.save_progress(60+int((i + 1) / total * 20))
                self.info.emit(f"Link ->  {url}")
                return result

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                self.tasks = [
                    asyncio.create_task(wrapped_scrape(i, browser, url))
                    for i, url in enumerate(fil)
                ]
                results=await asyncio.gather(*self.tasks, return_exceptions=True)
                await browser.close()
            if results and len(results) > 0:    
                for res in results:
                        linkovi.extend(list(res[0]))       # prvi set
                        tab_linkovi.extend(list(res[1]))   # drugi set
                        tabela.extend(res[2]) 
                        tab_linkovi1.extend(list(res[1]))

                self.tab_linkovi.extend(tab_linkovi) 
                self.tab_linkovi1.extend(tab_linkovi)  
                self.tabela1.extend(tabela)
        except:
            return
        
    async def scrape_table_detail1(self):
        try:
            total = len(self.tab_linkovi1)
            async def wrapped_scrape(i, browser, url):
                if self.stop:
                    self.results1.emit(self.tabela1)
                    self.results2.emit(self.tabela2)
                    return []
                result = await self.scrape_table_detail(browser, url)
                self.progress.emit(80+int((i + 1) / total * 20))
                self.save_progress(80+int((i + 1) / total * 20))
                self.info.emit(f"Link ->  {url}")
                return result
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                self.tasks = [
                    asyncio.create_task(wrapped_scrape(i, browser, url))
                    for i, url in enumerate(self.tab_linkovi1[:20])
                ]
                results=await asyncio.gather(*self.tasks, return_exceptions=True)
                await browser.close()
            if results and len(results) > 0:
                for tab in results:
                    self.tabela2.extend(tab)
        except:
            return
    async def export_Google_Sheet(self):
        df = pd.DataFrame(self.tabela2)
        if not df.empty:
            try:
                exporter=GoogleSheetsExporter()
                exporter.export_dataframe(df)
            except:
                self.error.emit("Nije moguće pristupiti Google Sheets")
        else:
            self.error.emit("Nema podataka i nije moguće izvršiti eksport u Google Sheets")

    async def export_File(self):
        df = pd.DataFrame(self.tabela2)
        if not df.empty:
            try:
                exporter=Exporter()
                exporter.export_dataframe(df)
            except:
                self.error.emit("Nije moguće pristupiti foderu za ekport .json .csv .xlsx fajlova")
        else:
            self.error.emit("Nema podataka i nije moguće izvršiti eksport u .json .csv .xlsx fajlove")

#                    POKRETANJE SCRAPARA VREMENSKI, AKO GUI DOZVOLI

def vrijeme():
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        try:
            df = pd.read_json(config_path)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            df["time"] = timestamp  # Dodaje novu kolonu 'run' u svaki red
            df.to_json(config_path, orient="records", indent=4, force_ascii=False)
            print("Upisano vrijeme.")
        except Exception as e:
            print("Nije upisano kontrolno vrijeme:", e)      

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    try:
        df = pd.read_json (config_path)
        return df
    except Exception as e:
        print("Greška pri učitavanju .json fajla:", e)
        df = pd.DataFrame([{
        "auto": False,
        "interval": 3600
        }])
        return df

def get_status():
    df=load_config()
    return  bool(df.loc[0, 'auto'])

def get_interval():
    df=load_config()
    print("Pročitao interval")
    return (df.loc[0, 'interval'])*1000


if __name__ == "__main__":
    app = QApplication([])

    # Timer koji paralelno ciklično poziva funkciju vrijeme()
    timer = QTimer()
    timer.timeout.connect(vrijeme)
    timer.start(60000)  # svakih 60 sekundi
    QTimer.singleShot(0, vrijeme)

    def StartAuto():
        """Pokreće jedan ciklus scraping-a i čeka thread"""
        if not get_status():
            print("⛔ Scraping zaustavljen.")
            app.quit()
            return

        # Kreiraj thread i worker
        thread = QThread()
        worker = Worker()
        worker.moveToThread(thread)

        # Poveži signal-slot
        thread.started.connect(worker.run)
        worker.finished.connect(thread.quit)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(worker.deleteLater)

        # Lokalni event loop za čekanje thread-a
        loop = QEventLoop()
        thread.finished.connect(loop.quit)

        thread.start()
        print("⏳ Čekam da Worker završi...")
        loop.exec()  # Čekamo da Worker završi
        print("✅ Worker završen")
        if not get_status():
            print("⛔ Scraping zaustavljen.")
            app.quit()
            return
        # Nakon što thread završi, zakazujemo sljedeći ciklus sa intervalom
        interval = get_interval()
        print(f"⏳ Čekam {interval} ms prije sljedećeg ciklusa")
        QTimer.singleShot(interval, StartAuto)  # start sljedećeg ciklusa

    # Start prvog ciklusa odmah
    QTimer.singleShot(get_interval(), StartAuto)
    app.exec()
    



  


