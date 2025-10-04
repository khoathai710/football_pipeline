import pandas as pd
from dagster import asset, Output, Definitions, AssetIn,AssetOut,multi_asset,DailyPartitionsDefinition

from etl_pipeline.resources.minio_io_manager import MinIOIOManager
from etl_pipeline.resources.mysql_io_manager import MySQLIOManager
from etl_pipeline.resources.psql_io_manager import PostgreSQLIOManager
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import time, tempfile
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

from selenium.common.exceptions import TimeoutException
from dagster import asset, StaticPartitionsDefinition

import re
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

DRIVER_PATH = ChromeDriverManager().install()

def crawl_matches_info(context, driver, link):
    wait = WebDriverWait(driver, 20)
    all_matches = []

    prefix = link

    data = {
        "match_id": link.split('/')[-2],
        "goals": [],
        "assists": [],
        "yellow_cards": [],
        "red_cards": [],
        "startings": [],
        "subs": [],
        "motm": None
    }

    # --- Goals, Assists, Yellow & Red Cards ---
    try:
        driver.get(f"{prefix}?tab=stats")
        #wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul[data-testid='homeTeamGoals']")))
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        if not soup.text.strip():
            context.log.error(f"Trang {link} không load được nội dung")
            return pd.DataFrame()

        # home goals & assists
        try:
            for goal in soup.find_all('ul', {"data-testid": "homeTeamGoals"}):
                # goals
                for g in goal.find_all('span', {"data-testid": 'scoreboardEventScorer'}):
                    try:
                        name = g.contents[0].strip()
                        minute = g.find("span").get_text(strip=True)
                        data["goals"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse home goal: {e}")
                # assists
                for g in goal.find_all('span', {"data-testid": 'scoreboardEventAssists'}):
                    try:
                        raw_text = g.get_text(strip=True).replace("(Assist)", "").strip()
                        parts = raw_text.rsplit(" ", 1)
                        name, minute = parts[0], parts[1]
                        data["assists"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse home assist: {e}")
        except Exception as e:
            context.log.warning(f"Lỗi parse homeTeamGoals block: {e}")

        # away goals & assists
        try:
            for goal in soup.find_all('ul', {"data-testid": "awayTeamGoals"}):
                # goals
                for g in goal.find_all('span', {"data-testid": 'scoreboardEventScorer'}):
                    try:
                        name = g.contents[0].strip()
                        minute = g.find("span").get_text(strip=True)
                        data["goals"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse away goal: {e}")
                # assists
                for g in goal.find_all('span', {"data-testid": 'scoreboardEventAssists'}):
                    try:
                        raw_text = g.get_text(strip=True).replace("(Assist)", "").strip()
                        parts = raw_text.rsplit(" ", 1)
                        name, minute = parts[0], parts[1]
                        data["assists"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse away assist: {e}")
        except Exception as e:
            context.log.warning(f"Lỗi parse awayTeamGoals block: {e}")

        # yellow cards (home + away)
        try:
            for card in soup.find_all('ul', {"data-testid": "homeTeamYellowCards"}):
                for c in card.find_all('li', {"data-testid": 'scoreboardCardEvent'}):
                    try:
                        raw_text = c.get_text(strip=True).strip()
                        parts = raw_text.rsplit(" ", 1)
                        name, minute = parts[0], parts[1]
                        data["yellow_cards"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse home yellow card: {e}")
        except Exception as e:
            context.log.warning(f"Lỗi parse homeTeamYellowCards block: {e}")

        try:
            for card in soup.find_all('ul', {"data-testid": "awayTeamYellowCards"}):
                for c in card.find_all('li', {"data-testid": 'scoreboardCardEvent'}):
                    try:
                        raw_text = c.get_text(strip=True).strip()
                        parts = raw_text.rsplit(" ", 1)
                        name, minute = parts[0], parts[1]
                        data["yellow_cards"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse away yellow card: {e}")
        except Exception as e:
            context.log.warning(f"Lỗi parse awayTeamYellowCards block: {e}")

        # red cards (home + away)
        try:
            for card in soup.find_all('ul', {"data-testid": "homeTeamRedCards"}):
                for c in card.find_all('li', {"data-testid": 'scoreboardCardEvent'}):
                    try:
                        raw_text = c.get_text(strip=True).strip()
                        parts = raw_text.rsplit(" ", 1)
                        name, minute = parts[0], parts[1]
                        data["red_cards"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse home red card: {e}")
        except Exception as e:
            context.log.warning(f"Lỗi parse homeTeamRedCards block: {e}")

        try:
            for card in soup.find_all('ul', {"data-testid": "awayTeamRedCards"}):
                for c in card.find_all('li', {"data-testid": 'scoreboardCardEvent'}):
                    try:
                        raw_text = c.get_text(strip=True).strip()
                        parts = raw_text.rsplit(" ", 1)
                        name, minute = parts[0], parts[1]
                        data["red_cards"].append({"name": name, "minute": minute})
                    except Exception as e:
                        context.log.warning(f"Lỗi parse away red card: {e}")
        except Exception as e:
            context.log.warning(f"Lỗi parse awayTeamRedCards block: {e}")

        driver.back()
    except Exception as e:
        context.log.error(f"Stats parse error: {e}")

    # --- Match Info ---
    try:
        # driver.get(f"{prefix}?tab=match+info")
        # wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='matchDetailsEntry']")))

        # soup = BeautifulSoup(driver.page_source, "html.parser")
        # if not soup.text.strip():
        #     context.log.error(f"Trang {link} không load được nội dung")
        #     return pd.DataFrame()

        # infos = soup.find_all('div', {'data-testid': 'matchDetailsEntry'})
        # for info in infos:
        #     key = info.find('span', class_='match-details__key').get_text(strip=True)
        #     value = info.find('span', class_='match-details__value').get_text(strip=True)
        #     data[key] = value

        # driver.back()
        pass
    except Exception as e:
        context.log.error(f"Match info parse error: {e}")

    # --- Lineups ---
    try:
        driver.get(f"{prefix}?tab=lineups")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        time.sleep(4)
        
        if not soup.text.strip():
            context.log.error(f"Trang {link} không load được nội dung")
            return pd.DataFrame()

        # đội hình xuất phát
        teams = soup.find_all("div", {"data-testid": "lineupsTeamFormation"})
        for team in teams:
            players = team.find_all("div", class_="lineups-player__headshot")
            for p in players:
                img = p.find("img")
                if img:
                    player_id = img["src"].split('/')[-1].split('.')[0]
                    data["startings"].append(player_id)

        # cầu thủ dự bị có vào sân
        sub_players = soup.find_all("ul", {"data-testid": "squadList"})
        for team in sub_players:
            players = team.find_all('a', class_="squad-list__item-link")
            for player in players:
                if player.find('div', {"data-testid": "lineupsPlayerSubOnBadge"}) is not None:
                    img = player.find("img")
                    if img:
                        player_id = img["src"].split('/')[-1].split('.')[0]
                        data["subs"].append(player_id)

        driver.back()
    except Exception as e:
        context.log.error(f"Lineups parse error: {e}")

    # --- MOTM ---
    try:
        driver.get(f"{prefix}?tab=recap")
        #wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.motm__player-name--first-name")))
        time.sleep(4)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")

        motm_img = soup.find('div', {"data-testid": "playerHeadshot"}).find("img")
        if motm_img:
            data['motm'] = motm_img["src"].split('/')[-1].split('.')[0]

        driver.back()
    except Exception as e:
        context.log.error(f"MOTM parse error: {e}")

    all_matches.append(data)
    result_df = pd.json_normalize(all_matches)
    return result_df

# Lấy kết quả của các trận đấu
weekly_partitions_def = StaticPartitionsDefinition(
    [str(i) for i in range(1,31)]
)

@asset(
    partitions_def=weekly_partitions_def,
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "football"],
    compute_kind="MinIO",
    group_name="bronze",
)
def bronze_matches_dataset(context) -> Output[pd.DataFrame]:
    week = int(context.partition_key)
    context.log.info(f"Khởi chạy asset cho Matchweek {week}")

    # Setup driver
    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options)
        context.log.info("Đã tạo driver thành công, chuẩn bị truy cập trang fixtures")
        driver.get("https://www.premierleague.com/fixtures")
    except Exception as e:
        context.log.error(f"Lỗi khi khởi tạo driver hoặc truy cập trang: {e}")
        raise

    wait = WebDriverWait(driver, 100)

    # Accept cookies
    try:
        accept_btn = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        accept_btn.click()
        context.log.info("Đã bấm nút chấp nhận cookies")
    except TimeoutException:
        context.log.error("Không tìm thấy nút chấp nhận cookies, bỏ qua")
    except Exception as e:
        context.log.error(f"Lỗi khi xử lý cookies: {e}")

    # Close popup
    try:
        close_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Close Sheet']")))
        try: 
            driver.execute_script("arguments[0].click();", close_btn)
            context.log.info("Đã đóng pop-up")
        except:
            context.log.info("Đã fail đóng pop-up")
    except Exception as e:
        context.log.error(f"Lỗi khi đóng pop-up: {e}")
        raise

    # Mở filter Matchweek
    try:
        filter_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Filter By: '][.//span[contains(text(),'MW') or contains(text(),'Matchweek')]]")))
        try: 
            driver.execute_script("arguments[0].click();", filter_btn)
            context.log.info("Đã ấn nút filter để chọn Matchweek")
        except:
            context.log.info("Đã fail ấn nút filter để chọn Matchweek")
    
        
    except Exception as e:
        context.log.error(f"Lỗi khi mở filter Matchweek: {e}")
        raise

    # Chọn Matchweek
    try:
        mw_label = wait.until(EC.element_to_be_clickable((By.XPATH, f"//label[@for='fixtures_matchweek_{week - 1}']")))
        try:
            driver.execute_script("arguments[0].click();", mw_label)
            context.log.info(f"Đã chọn thành công Matchweek {week}")
        except:
            context.log.info("Đã fail chọn thành công Matchweek {week}")
        
    except Exception as e:
        context.log.error(f"Lỗi khi chọn Matchweek {week}: {e}")
        raise

    # Save filter
    try:
        save_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Save']]")))
        try: 
            driver.execute_script("arguments[0].click();", save_btn)
            context.log.info("Đã bấm Save, chờ load dữ liệu")
            
        except:
            context.log.info("Đã fail bấm Save, chờ load dữ liệu")
       
    except Exception as e:
        context.log.error(f"Lỗi khi bấm Save: {e}")
        raise

    # Lấy HTML
    try:
        time.sleep(3)
        html = driver.page_source
        #driver.quit()
        context.log.info("Đã lấy source HTML và đóng driver")
    except Exception as e:
        context.log.error(f"Lỗi khi lấy HTML hoặc đóng driver: {e}")
        raise

    data = []
    if not html:
        context.log.error("Không lấy được HTML từ trang")
    else:
        try:
            context.log.info("Bắt đầu parse HTML với BeautifulSoup")
            soup = BeautifulSoup(html, "html.parser")
            dates = soup.find_all('div', class_='match-list__day-matches')
            

            for date in dates:
                day = date.find('span', class_='match-list__day-date').get_text(strip=True)
                matches = date.find_all("a", {"data-testid": "matchCard"})
                context.log.info(f"Tìm thấy {len(matches)} trận đấu vào ngày {day}")
                

                for match in matches:
                
                    badges = match.find_all("span", {"data-testid": "matchCardTeamBadge"})
                    home = None
                    away = None

                    if len(badges) >= 2:
                        home_img = badges[0].find("img")
                        away_img = badges[1].find("img")
                        if home_img and home_img.has_attr("src"):
                            home = home_img["src"].split('/')[-1].split('.')[0]
                        if away_img and away_img.has_attr("src"):
                            away = away_img["src"].split('/')[-1].split('.')[0]
                    context.log.info(f'Bắt đầu thực hiện crawl cho trận đấu {home} - {away} - {day}')
                    score = match.find('span', class_='match-card__score-label').get_text(strip=True)
                    link = match['href']
                    id = link.split('/')[-2]
                    link = 'https://www.premierleague.com'+ match['href']
                    #context.log.info(f'{home}-{away}')
                    match_info = crawl_matches_info(context,driver,link)
                    data.append({
                        "id": id,
                        "date": day,
                        "home": home,
                        "away": away,
                        "score": score,
                        "goals": match_info["goals"].iloc[0],
                        'assists': match_info["assists"].iloc[0],
                        "yellow_cards": match_info["yellow_cards"].iloc[0],
                        "red_cards": match_info["red_cards"].iloc[0],
                        "startings": match_info["startings"].iloc[0],
                        "subs": match_info["subs"].iloc[0],
                        "motm": match_info["motm"].iloc[0]
                    })
                

            context.log.info(f"Tổng số trận đấu thu thập được: {len(data)}")
        except Exception as e:
            context.log.error(f"Lỗi khi parse HTML: {e}")
            raise

    # Convert DataFrame
    try:
        df = pd.DataFrame(data, columns=[
            'id','date','home','away','score','goals','assists','yellow_cards','red_cards','startings','subs','motm'
        ])
        context.log.info(f"Đã tạo DataFrame với {len(df)} bản ghi")
    except Exception as e:
        context.log.error(f"Lỗi khi tạo DataFrame: {e}")
        raise

    return Output(
        df,
        metadata={
            "table": "premier_league_matches",
            "records_count": len(df),
        }
    )

@asset(
    group_name='bronze',
    io_manager_key= "minio_io_manager",
    required_resource_keys={'mysql_io_manager'},
    key_prefix=['bronze','football'],
    compute_kind = 'MySQL',
)
def bronze_football_stadiums_dataset(context)->Output:
    table  = 'football_stadiums'
    sql = f'SELECT * FROM {table}'
    context.log.info(f"Extract data from {table}")
    pd_data = context.resources.mysql_io_manager.extract_data(sql)
    context.log.info(f"Extract successfully from {table}")
        
    return Output(
        pd_data, 
        metadata={
            "table": table, 
            "column_count": len(pd_data.columns),
            "records": len(pd_data)
            }
        )

def crawl_team(team_id: str, context) -> pd.DataFrame:
    

    context.log.info(f"🚀 Bắt đầu crawl team_id={team_id}")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")        
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    service = Service(DRIVER_PATH)  # dùng lại path đã tải
    
    driver = webdriver.Chrome(service=service, options=options)
    context.log.info('Tạo session driver thành công')
    wait = WebDriverWait(driver, 10)

    url = f"https://www.premierleague.com/en/players?competition=8&season=2025&team={team_id}"
    driver.get(url)

    try:
        accept_btn = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        accept_btn.click()
    except TimeoutException:
        pass

    results = []
    while True:
        try:
            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "player-listings-row")))
        except TimeoutException:
            break

        soup = BeautifulSoup(driver.page_source, "html.parser")
        players = soup.find_all('tr', class_='player-listings-row')

        for player in players:
            name = player.find('p', class_='player-listings-row__player-name').get_text(strip=True)
            club = player.find('td', class_='player-listings-row__data--club').get_text(strip=True)
            position = player.find('td', class_='player-listings-row__data--position').get_text(strip=True)
            nation = player.find('td', class_='player-listings-row__data--nationality').get_text(strip=True)

            img_tag = player.find("img")
            img_url = img_tag["src"] if img_tag else None
            if img_url:
                lists = img_url.split('/')
                lists[-2] = '110x140'
                new_url = '/'.join(lists)
            else:
                new_url = None

            results.append({
                "team_id": team_id,
                "player_id": img_url.split('/')[-1].split('.')[0],
                "name": name,
                "club": club,
                "position": position,
                "nation": nation,
                "img_url": new_url,
            })

        # Next page
        try:
            next_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Next"]')))
            if next_button.get_attribute("disabled"):
                break
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(1)
        except TimeoutException:
            break

    driver.quit()

    context.log.info(f"✅ Crawl xong team_id={team_id}, tổng số {len(results)} cầu thủ")
    return pd.DataFrame(results)

@asset(
    group_name='bronze',
    io_manager_key="minio_io_manager",
    key_prefix=['bronze', 'football'],
    compute_kind='MinIO',
)
def bronze_player_info(context) -> Output[pd.DataFrame]:
    context.log.info(DRIVER_PATH)
    team_param = "54,3,7,91,94,36,90,8,31,11,2,14,43,1,4,17,56,6,21,39"
    #team_param = "54"
    teams = team_param.split(",")

    dfs = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(crawl_team, t, context): t for t in teams}
        for f in as_completed(futures):
            df = f.result()
            
            dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)
    context.log.info(f"🎯 Crawl xong toàn bộ {len(teams)} đội, tổng số {len(final_df)} cầu thủ")

    return Output(final_df)

@asset(
    group_name='bronze',
    io_manager_key="minio_io_manager",
    key_prefix=['bronze', 'football'],
    compute_kind='MinIO',
)
def bronze_teams_dataset(context):
    team_param = "54,3,7,91,94,36,90,8,31,11,2,14,43,1,4,17,56,6,21,39"
    teams = team_param.split(",")

    data = []

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options)

    for team_id in teams:
        url = f"https://www.premierleague.com/en/clubs/{team_id}/overview"
        context.log.info(f"Fetching: {url}")
        driver.get(url)

        try:
            # đợi tên đội xuất hiện (tối đa 10 giây)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.club-profile-header__title"))
            )

            soup = BeautifulSoup(driver.page_source, "html.parser")

            name = soup.select_one("h1.club-profile-header__title").get_text(strip=True)
            link = soup.select_one("div.analytics-link a")["href"]
            info = soup.select("p.club-profile-bio__metadata-value")
            est = info[0].get_text(strip=True) if len(info) > 0 else None
            #stadium = info[1].get_text(strip=True) if len(info) > 1 else None

            data.append({
                "team_id": team_id,
                "name": name,
                "link": link,
                "est": est,
                
            })

        except Exception as e:
            print(f"❌ Lỗi với team_id={team_id}: {e}")
   
    driver.quit()
    df = pd.DataFrame(data, columns=["team_id", "name", "link", "est"])
    return Output(
        df,
        metadata={
            'row_count': df.shape[0]
        }
    ) 