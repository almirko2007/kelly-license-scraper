import asyncio
import random
import re
from PySide6.QtCore import QObject, Signal
from datetime import datetime
from playwright.async_api import async_playwright
from Playwright_Capatatcha import Stealth

class DetailScraper (QObject):
    error = Signal(str)
    MAX_ATTEMPTS = 3
    BASE_DELAY = 2.0
    MAX_BACKOFF = 60.0


    def __init__(self,use_proxy=False):
        super().__init__()

        self.proxy_list = [
            None,
            #"http://1.2.3.4:8000",
            #("http://12.34.56.78:8000", "user1", "pass1"),
            #"socks5://5.6.7.8:1080",
        ]
        self.semaphore = asyncio.Semaphore(5)
        self.keyword = ""
        self.use_proxy = use_proxy
        self.stealth = Stealth(proxy_list = self.proxy_list)

    def compute_backoff_with_full_jitter(self, attempt):
        cap = min(self.MAX_BACKOFF, self.BASE_DELAY * (2 ** attempt))
        return random.uniform(0, cap)
            
    async def scrape_links(self, browser, url):
        print(f"Startam...")
        for attempt in range(self.MAX_ATTEMPTS):            
            context = None
            page = None
            try:
                context = await self.stealth.create_context_with_stealth(
                        browser, use_proxy=self.use_proxy)
                page = await context.new_page()

                await page.mouse.move(300, 400)
                await page.wait_for_timeout(random.randint(500, 1500))
                await page.mouse.move(600, 500)
                await page.wait_for_timeout(random.randint(200, 1200))

                for nav_try in range(3):
                    try:
                        response = await page.goto(url, timeout=60000)
                        if response and response.status >= 500:
                            print(f"Server error {response.status}, retrying...")
                            await page.wait_for_timeout(3000)
                            continue
                        break
                    except Exception as e:
                        print(f"Pokušaj {nav_try+1} nije uspio: {e}")
                        await page.wait_for_timeout(3000)
                        
                if await self.stealth.is_captcha_page(page):
                        print("⚠ CAPTCHA je detektovana!")

                        # automatski pokušaji
                        if attempt < self.MAX_ATTEMPTS-2:
                            await page.screenshot(path=f"captcha_detected_{attempt}.png")
                            print(f"📸 Screenshot spremljen: captcha_detected_{attempt}.png")

                            try:
                                await context.close()
                            except Exception:
                                pass  # u slučaju da su već zatvoreni

                            delay = self.compute_backoff_with_full_jitter(attempt)
                            print(f"🔁 Ponavljam pokušaj za {delay:.1f} sekundi...")
                            await asyncio.sleep(delay)
                            continue

                        # ako iscrpiš pokušaje – ručni mod
                        print("⏳ Višestruki neuspjesi — prelazim na ručno rješavanje CAPTCHA.")
                        solved = await self.stealth.wait_for_captcha(page, timeout=300)

                        if not solved:
                            print("❌ CAPTCHA nije riješena, prekidam rad.")
                            await context.close()
                            return set()
                        else:
                            print("✅ CAPTCHA uspješno riješena, nastavljam dalje.")
                            

                try:
                    hrefs = await page.locator("a").evaluate_all("(elements) => elements.map(el => el.href)")
                except Exception as e:
                    print(f"Greška pri dohvaćanju linkova: {e}")
                    hrefs = []

                await context.close()

                exclude_keywords = [
                    "login", "signin", "auth", "account", "register",
                    "searchbyconame", "searchbylicense", "index", "searchbycity",
                    "google", "erenewals", "tonnage", "licensing", "applynow", "update"
                ]
                filtered = [
                    link for link in hrefs
                    if link.startswith("http") and not any(k in link.lower() for k in exclude_keywords)
                ]
                return set(filtered)

            except Exception as e:
                print(f"Greška u pokušaju {attempt+1}: {e}")
                try:
                    if context:
                        await context.close()
                except:
                    pass
                await asyncio.sleep(self.compute_backoff_with_full_jitter(attempt))

        print("Nakon više pokušaja, linkovi nisu dohvaćeni.")
        return set()
    
    async def scrape_detail(self,browser, url):
        for attempt in range(self.MAX_ATTEMPTS):

            async with self.semaphore:
                    context = None
                    page = None
                    try:
                        context = await self.stealth.create_context_with_stealth(
                            browser, use_proxy=self.use_proxy)
                        page = await context.new_page()

                        await page.mouse.move(300, 400)
                        await page.wait_for_timeout(random.randint(500, 1500))
                        await page.mouse.move(600, 500)
                        await page.wait_for_timeout(random.randint(200, 1200))

                        for nav_try in range(3):
                            try:
                                response = await page.goto(url, timeout=60000)
                                if response and response.status >= 500:
                                    print(f"Server error {response.status}, retrying...")
                                    await page.wait_for_timeout(3000)
                                    continue
                                break
                            except Exception as e:
                                print(f"Pokušaj {nav_try+1} nije uspio: {e}")
                                await page.wait_for_timeout(3000)


                        if await self.stealth.is_captcha_page(page):
                            print("⚠ CAPTCHA je detektovana!")

                            # automatski pokušaji
                            if attempt < self.MAX_ATTEMPTS-1:
                                await page.screenshot(path=f"captcha_detected_{attempt}.png")
                                print(f"📸 Screenshot spremljen: captcha_detected_{attempt}.png")

                                try:
                                    await context.close()
                                except Exception:
                                    pass  # u slučaju da su već zatvoreni

                                delay = self.compute_backoff_with_full_jitter(attempt)
                                print(f"🔁 Ponavljam pokušaj za {delay:.1f} sekundi...")
                                await asyncio.sleep(delay)
                                continue

                            # ako iscrpiš pokušaje – ručni mod
                            print("⏳ Višestruki neuspjesi — prelazim na ručno rješavanje CAPTCHA.")
                            solved = await self.stealth.wait_for_captcha(page, timeout=300)

                            if not solved:
                                print("❌ CAPTCHA nije riješena, prekidam rad.")
                                await context.close()
                                return set(), set(), []
                            else:
                                print("✅ CAPTCHA uspješno riješena, nastavljam dalje.")
                                
                        # Scraping flow
                        filtered_links = []
                        tab_links = []
                        tabela = []

                        try:
                            # simuliraj ljudsko ponašanje dodatno (scroll, click stub)
                            await page.evaluate("window.scrollTo({top: document.body.scrollHeight/3, behavior: 'smooth'})")
                            await page.wait_for_timeout(random.randint(700, 1400))

                            # Provjera i submit forme
                            submit_exists = await page.locator("input[type='submit']").count() > 0
                            city_exists = await page.locator("input[name='City']").count() > 0

                            if submit_exists and city_exists:
                                await page.fill("input[name='City']", self.keyword)
                                await page.click("input[type='submit']")
                                await page.wait_for_timeout(random.randint(800, 1800))

                            # Pokupi linkove na stranici
                            hrefs = await page.locator("a").evaluate_all(
                                "(elements) => elements.map(el => el.href)"
                            ) or []

                            table_locator = page.locator("table")
                            has_table = await table_locator.count()

                            if has_table and not re.search(r"login", url, re.IGNORECASE):
                                await page.wait_for_selector("table tr td", timeout=20000)
                                tables = await page.locator("table").all()

                                for table in tables:
                                    if not await table.is_visible():
                                        continue

                                    # Preskoči tabele koje imaju inpute
                                    if await table.locator("input[type='submit']").count()>0 or await table.locator("input[name='City']").count()>0 or await table.locator("input[name='Zip1']").count()>0 or await table.locator("input[name='County']").count()>0:
                                        print("Detektovan input")
                                        continue

                                    text = await table.inner_text()
                                    if "No records returned." in text:
                                        continue

                                    # Pokupi linkove iz tabele
                                    try:
                                        links_in_table = await table.locator("tbody tr td a").evaluate_all(
                                            "(els) => els.map(e => e.href)"
                                        )
                                    except Exception:
                                        links_in_table = []

                                    if links_in_table:
                                        tab_links.extend(links_in_table)
                                    else:
                                        headers = await table.locator("tr:first-child th, tr:first-child td").all_inner_texts()
                                        all_rows = await table.locator("tr").all()
                                        rows = all_rows[1:]

                                        try:
                                            type_lic = await page.locator("div.gda_container h2 center").inner_text()
                                        except Exception:
                                            type_lic = ""

                                        for row in rows:
                                            cells = await row.locator("td").all()
                                            tdict = {}
                                            for i, header in enumerate(headers):
                                                if i < len(cells):
                                                    font_count = await cells[i].locator("font").count()
                                                    value = await (
                                                        cells[i].locator("font").inner_text() if font_count > 0
                                                        else cells[i].inner_text()
                                                    )
                                                    tdict[header] = value
                                            tdict["Type of licenses"] = type_lic
                                            tabela.append(tdict)

                            # Filtriraj linkove koji sadrže "searchbyCity" i dealer
                            else:
                                filtered_links =[
                                    link for link in hrefs
                                    if link and re.search(r"(searchbyCity|dealer)", link, re.IGNORECASE)              
                                ]
                        except Exception as e:
                            print(f"Greška pri dohvaćanju linkova: {e}")
                            filtered_links = []

                        exclude_exact = ["https://www.kellysolutions.com/ga/searchbyCity.asp","https://www.kelly-products.com/kelly-registration-systems/online-licensing-and-agriculture-permits-for-businesses-dealers-and-individuals/"]
                        filtered_links = [link for link in filtered_links if link not in exclude_exact]

                        await context.close()
                        print("završio sam pregled stranice. ", url)
                        return set(filtered_links), set(tab_links), tabela

                    except Exception as e:
                        print(f"Greška u pokušaju {attempt+1}: {e}")
                        try:
                            if context:
                                await context.close()
                        except:
                            pass
                        await asyncio.sleep(self.compute_backoff_with_full_jitter(attempt))

        print("Nakon više pokušaja, CAPTCHA nije zaobiđena.")
        await context.close()
        return set(), set(), []
    
    async def scrape_table_detail(self,browser, url):
        for attempt in range(self.MAX_ATTEMPTS):

            async with self.semaphore:
                    context = None
                    page = None
                    try:
                        context = await self.stealth.create_context_with_stealth(
                            browser, use_proxy=self.use_proxy)
                        page = await context.new_page()

                        await page.mouse.move(300, 400)
                        await page.wait_for_timeout(random.randint(500, 1500))
                        await page.mouse.move(600, 500)
                        await page.wait_for_timeout(random.randint(200, 1200))

                        for nav_try in range(3):
                            try:
                                response = await page.goto(url, timeout=60000)
                                if response and response.status >= 500:
                                    print(f"Server error {response.status}, retrying...")
                                    await page.wait_for_timeout(3000)
                                    continue
                                break
                            except Exception as e:
                                print(f"Pokušaj {nav_try+1} nije uspio: {e}")
                                await page.wait_for_timeout(3000)

                        if await self.stealth.is_captcha_page(page):
                            print("⚠ CAPTCHA je detektovana!")

                            # automatski pokušaji
                            if attempt < self.MAX_ATTEMPTS-1:
                                await page.screenshot(path=f"captcha_detected_{attempt}.png")
                                print(f"📸 Screenshot spremljen: captcha_detected_{attempt}.png")

                                try:
                                    await context.close()
                                except Exception:
                                    pass  # u slučaju da su već zatvoreni

                                delay = self.compute_backoff_with_full_jitter(attempt)
                                print(f"🔁 Ponavljam pokušaj za {delay:.1f} sekundi...")
                                print("CAPATCHA se pojavila na stranici: ", url)
                                await asyncio.sleep(delay)
                                continue

                            # ako iscrpiš pokušaje – ručni mod
                            print("⏳ Višestruki neuspjesi — prelazim na ručno rješavanje CAPTCHA.")
                            solved = await self.stealth.wait_for_captcha(page, timeout=300)

                            if not solved:
                                print("❌ CAPTCHA nije riješena, prekidam rad.")
                                await context.close()
                                return []
                            else:
                                print("✅ CAPTCHA uspješno riješena, nastavljam dalje.")

                        tabela = []
                        await page.mouse.move(300, 400)
                        await page.wait_for_timeout(1500)
                        
                        if await page.locator("table").count()>0:
                            await page.wait_for_selector("table tr td", timeout=20000)
                            tables = await page.locator("table").all()
                            try:
                                type_lic = await page.locator("div.gda_container h2 center").inner_text()
                            except:
                                type_lic = ""
                            
                            for table in tables:
                                content = await table.inner_text()
                                if not content.strip():
                                    continue 

                                if not self.keyword.strip().lower() in content.lower():
                                    return []

                                rows = await table.locator("tr").all()
                                tab_dict = {}
                                for row in rows:
                                    content_r = await row.inner_text()
                                    if not content_r.strip():
                                        continue 
                                    try:
                                        cells = await row.locator("td").element_handles()
                                        cell_texts = [await cell.inner_text() for cell in cells]
                                        cell_texts = [text.strip() for text in cell_texts]
                                    except Exception as e:
                                        print(f"Greška pri čitanju reda: {e}")
                                        continue
                                    if len(cell_texts) == 1:
                                        tab_dict[cell_texts[0]] = ""
                                    elif len(cell_texts) >= 2:
                                        tab_dict[cell_texts[0]] = cell_texts[1]

                                    tab_dict["Type of licenses"] = type_lic
                                tabela.append(tab_dict)
                                print(tabela)

                        await context.close()
                        return tabela

                    except Exception as e:
                        print(f"Greška u pokušaju {attempt+1}: {e}")
                        try:
                            if context:
                                await context.close()
                        except:
                            pass
                        await asyncio.sleep(self.compute_backoff_with_full_jitter(attempt))
        print("Nakon više pokušaja, tabela nije dohvaćena.")
        await context.close()
        return []
    
