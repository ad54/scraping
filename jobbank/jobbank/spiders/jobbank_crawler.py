# -*- coding: utf-8 -*-
import re
import datetime
# import pdfkit
import scrapy
from jobbank.items import JobbankItem
from jobbank.config import *
from pprint import pprint as pp
import pymysql
import pandas as pd
import pyap

class JobbankCrawlerSpider(scrapy.Spider):
    name = 'jobbank_crawler'
    allowed_domains = []
    start_urls = ['https://www.jobbank.gc.ca/jobsearch/jobsearch?page=1&sort=D&fage=2']
    domain = 'https://www.jobbank.gc.ca'
    con =''
    cursor = ''
    max_page = 0

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.con = pymysql.connect(db_host, db_user, db_password)
        self.cursor = self.con.cursor()
        self.cursor.execute('CREATE DATABASE IF NOT EXISTS ' + db_name)
        self.con = pymysql.connect(db_host, db_user, db_password, db_name)
        self.cursor = self.con.cursor()

        try:
            create_table = "CREATE TABLE IF NOT EXISTS " + db_output_table+ """ (advertisedUntil varchar(50),
                                                                                datePosted varchar(50),
                                                                                anticipatedStartDate varchar(50),
                                                                                city varchar(250),
                                                                                email varchar(250),
                                                                                Employer longtext,
                                                                                jobNumber varchar(50),
                                                                                Id int,
                                                                                jobOfferURL longtext,
                                                                                language varchar(250),
                                                                                location longtext,
                                                                                medianWage varchar(250),
                                                                                noc int,
                                                                                numberOfPositions varchar(250),
                                                                                outlook int,
                                                                                phone varchar(25),
                                                                                postalCode varchar(25),
                                                                                province varchar(250),
                                                                                region varchar(250),
                                                                                salary varchar(250),
                                                                                similarJobsNb int,
                                                                                    source varchar(250),
                                                                                title longtext,
                                                                                process_date date,
                                                                                year_of_experience varchar(250),
                                                                                education varchar(250),
                                                                                emp_grp varchar(250),
                                                                                emp_cond varchar(250),
                                                                                benefits varchar(250),
                                                                                period_of_emp varchar(250),
                                                                                hour_of_work varchar(250)
                                                                                
                                                                                
                                                                                        )"""
            self.cursor.execute(create_table)

        except Exception as e:
            print("Can't Create table :" + str(e))

    def start_requests(self):
        try:
            get_job_id = f'select Id from {db_output_table}'
            self.cursor.execute(get_job_id)
            sql_ids = self.cursor.fetchall()
            sql_id_list = list()
            for sql_id in sql_ids:
                sql_id_list.append(sql_id[0])
            self.sql_ids = set(sql_id_list)
            url = 'https://www.jobbank.gc.ca/jobsearch/jobsearch?page=1&sort=D&fage=2'
            yield scrapy.FormRequest(url=url,callback=self.parse)
        except Exception as e:
            print(e)

    def parse(self, response):
        try:
            job_urls = response.xpath('//a[@class="resultJobItem"]/@href').getall()

            JobIds = list()
            for job_url in job_urls:
                url_splitter = ';' if ';' in job_url else '?'
                JobIds.append(int(job_url.split('jobposting/')[1].split(url_splitter)[0]))

            JobIds = set(JobIds)
            print(len(JobIds))
            JobIds = JobIds - self.sql_ids
            print(len(JobIds))

            for JobId in JobIds:
                url = f"https://www.jobbank.gc.ca/jobsearch/jobposting/{JobId}?source=searchresults"

                yield scrapy.FormRequest(url=url, callback=self.get_job_data, meta={'JobId':JobId})
                # break

            next_page_url = response.xpath('//ul[@class="pagination"]/li[@class="active"]/following-sibling::li/a/@href').get(default='')
            if next_page_url:
                next_page_url = (self.start_urls[0].split('?page')[0]) + next_page_url
                yield scrapy.FormRequest(url=next_page_url,callback=self.parse)
            #     self.max_page += 1

        except Exception as e:
            print(e)

    def get_job_data(self, response):
        if is_save_html:
            file_path = f"{html_data_directory}{(response.meta.get('JobId', ''))}.html"
            with open(file_path, 'wb') as f:
                f.write(response.body)
        if is_save_pdf:
            pdf_path = f"{pdf_directory}{(response.meta.get('JobId', ''))}.pdf"
            pdfkit.from_url(response.url, pdf_path)

        try:
            item = JobbankItem()
            item['advertisedUntil'] = response.xpath('normalize-space(//p[@property="validThrough"]/text())').get(default='')
            datePosted = (response.xpath('normalize-space(//span[@property="datePosted"]/text())').get(default='')).split('on ')[-1]
            if datePosted:
                item['datePosted'] = datetime.datetime.strftime(datetime.datetime.strptime(datePosted, "%B %d, %Y"), "%Y-%m-%d")

            item['anticipatedStartDate'] =  response.xpath('normalize-space(//span[contains(text(),"Start date")]/following-sibling::span/text())').get(default='')
            item['city'] = response.xpath('normalize-space(//span[@property="addressLocality"]/text())').get(default='')
            item['email'] = response.xpath('normalize-space(//div[@id="seekeractivity:howtoapply"]//a[contains(@href,"mailto")]/text())').get(default='')
            item['Employer'] = (''.join(response.xpath('//span[@property="hiringOrganization"]/span[@property="name"]//text()').getall()))
            item['Employer'] = re.sub('\r|\t|\n','',item['Employer'])
            item['jobNumber'] = response.xpath('normalize-space(//span[contains(text(),"Job no.")]/following-sibling::span[2]/text())').get(default='')
            item['Id'] = response.url.split('jobposting/')[1].split('?')[0]
            item['jobOfferURL'] = response.url
            item['language'] = response.xpath('normalize-space(//div[@class="job-posting-detail-requirements"]/h4[contains(text(),"Languages")]/following-sibling::p/text())').get(default='')
            item['medianWage'] = response.xpath('normalize-space(//dt[contains(text(),"Median wage")]/following-sibling::dd[1]/a/text())').get(default='')
            item['noc'] = re.sub('\D','',(response.xpath('normalize-space(//span[@class="noc-no"]/text())').get(default='')))
            item['numberOfPositions'] = response.xpath('normalize-space(//span[contains(text(),"Vacancies")]/following-sibling::span/text())').get(default='')
            item['outlook'] = re.findall('\d+',(response.xpath('normalize-space(//span[contains(@class,"star-outlook-")]/../@title)').get(default='')))
            item['outlook'] = item['outlook'][0] if item['outlook'] else ''
            item['phone'] =  response.xpath('normalize-space(//div[@class="job-posting-detail-apply"]//h4[contains(text(),"Phone")]/following-sibling::p[1]/text())').get(default='')
            item['location'] = response.xpath('normalize-space(//div[@class="job-posting-detail-apply"]//h4[contains(text(),"Job location")]/following-sibling::p[1]/text())').get(default='')

            item['province'] = response.xpath('normalize-space(//span[@property="addressRegion"]/text())').get(default='')
            item['region'] = response.xpath('normalize-space(//span[@class="noc-location"]/text())').get(default='')
            QuantitativeValue = ' '.join(response.xpath('//span[@typeof="QuantitativeValue"]//text()').getall())
            workHours = response.xpath('normalize-space(//span[@property="workHours"]/text())').get(default='')
            item['salary'] = QuantitativeValue + workHours
            item['similarJobsNb'] = response.xpath('count(//div[contains(@class,"job-posting-details-similar-jobs")]/ul/li)').get(default='')
            item['source'] = ''.join(response.xpath('//span[contains(text(),"Source")]/following-sibling::span//text()[normalize-space()]').getall())
            if item['source']:
                item['source'] = re.sub('\s+' , ' ',re.sub('\r|\t|\n',' ',item['source'])).strip()
            item['title'] = response.xpath('normalize-space(//span[@property="title"]/text())').get(default='')


            item['year_of_experience'] = response.xpath('//h4[contains(text(),"Experience")]/following-sibling::p/text()').get(default='')
            item['education'] = response.xpath('//h4[contains(text(),"Education")]/following-sibling::p/text()').get(default='')
            item['emp_grp'] = response.xpath('//h3[contains(text(),"Employment groups")]/following-sibling::p/strong/text()').get(default='')
            item['emp_cond'] = response.xpath('//span[@property="specialCommitments"]/text()').get(default='')
            item['benefits'] = response.xpath('//span[@property="benefits"]/text()').get(default='')
            period_of_emp = item['period_of_emp'] = re.sub('\s+' , ' ',re.sub('\r|\t|\n',' ',(','.join(response.xpath('//span[@property="employmentType"]/text()').getall())))).strip()
            if 'full' in period_of_emp.lower():
                item['hour_of_work'] = 'Full Time'
            elif 'part' in period_of_emp.lower():
                item['hour_of_work'] = 'Part Time'
            else:
                item['hour_of_work'] = 'unknown'


            if item['location']:
                addresses = (pyap.parse(item['location'], country='CA'))
                if addresses:
                    address = addresses[0]
                    parsed_address = address.as_dict()
                    item['postalCode'] = parsed_address.get('postal_code','')
            item['process_date'] = today

            yield item
        except Exception as e:
            print(e)

from scrapy.cmdline import execute
execute('scrapy crawl jobbank_crawler'.split())