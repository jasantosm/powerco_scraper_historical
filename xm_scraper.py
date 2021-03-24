import pandas as pd
import time
from bs4 import BeautifulSoup

from IPython.display import display_html
from selenium import webdriver
from mysql_service import insert_day


def selenium_scraper():
    url = 'http://ido.xm.com.co/ido/SitePages/ido.aspx'
    sleep_time = 3

    options = webdriver.ChromeOptions()
    options.add_argument('--incognito')

    driver = webdriver.Chrome(executable_path = '/Users/juliansantos/Documents/electrical_power_system_CO/electrical_power_system_CO/chromedriver',
                            options = options)

    driver.get(url)
    time.sleep(sleep_time)

    #Selenium Date Inputbox selection in browser

    try:
        date_box = driver.find_elements_by_id('report-date')
        date_box[0].clear()
    except Exception as e:
        print('Error: ')
        print(e)
        print('-*-'*50)
        
        
    #Startdate initialization

    date = pd.to_datetime('02/10/2020', format='%d/%m/%Y')
    finish_date = pd.to_datetime('03/10/2020', format='%d/%m/%Y') + pd.DateOffset(days=1) #adds 1 day to the date

    while date != finish_date:
        try:
            date_box[0].send_keys(date.strftime('%d/%m/%Y'))

            date_button = driver.find_elements_by_xpath('//div[@id="filter-button"]/button')
            date_button[0].click()
            time.sleep(sleep_time)


            #Tables titles scraping

            scraped_table_titles = driver.find_elements_by_xpath('//div[@class="text-blue textL"]/b')

            scraped_table_titles.pop(0)

            table_titles_string = ''

            for table_title in scraped_table_titles:
                table_titles_string = table_titles_string + table_title.text + '|'


            tables = driver.find_elements_by_xpath('//table[@class="report-table"]')
            aportes_x = driver.find_elements_by_xpath('//table[@id="table-aportes-x"]/tbody')
            reservas_x = driver.find_elements_by_xpath('//table[@id="table-reservas-x"]/tbody')

            html_tables_no_open = ''

            for scraped_table in tables:

                if scraped_table.get_attribute('id') == 'table-aportes-x':
                    if (aportes_x[0].get_attribute('innerHTML') != ''):
                        html_tables_no_open = html_tables_no_open + scraped_table.get_attribute('outerHTML') + '^_^'
                    else:
                        continue
                elif scraped_table.get_attribute('id') == 'table-reservas-x':
                    if (reservas_x[0].get_attribute('innerHTML') != ''):
                        html_tables_no_open = html_tables_no_open + scraped_table.get_attribute('outerHTML') + '^_^'
                    else:
                        continue
                else:
                    aportes_vacio = False
                    reservas_vacio = False
                    soup = BeautifulSoup(scraped_table.get_attribute('innerHTML'), 'lxml')
                    td = soup.find('td')
                    tbody = soup.find('tbody')
                    #print(td)
                    if str(td)== '<td>Rio</td>':
                        if str(tbody) == '<tbody class="report-table-body"></tbody>':
                            aportes_vacio = True
                            print('Entra aportes_vacio')
                    if str(td)== '<td> Embalse </td>':
                        if str(tbody) == '<tbody class="report-table-body"></tbody>':
                            reservas_vacio = True
                            print('entra reservas vacio')
                        
                    if aportes_vacio:
                        continue
                    elif reservas_vacio:
                        continue
                    else:
                        html_tables_no_open = html_tables_no_open + scraped_table.get_attribute('outerHTML') + '^_^'

            print('--'*30)
            print('Scrapiado: ' + str(date))
            print('--'*30)
            print('\n')

            insert_day(date, table_titles_string, html_tables_no_open)
            date = date + pd.DateOffset(days=1) #adds 1 day to the date
            
        except Exception as e:
            print('Error: ')
            print(e)
            print('date: '+str(date))
            print('-*-'*30)
            continue

if __name__ == "__main__":
    selenium_scraper()

