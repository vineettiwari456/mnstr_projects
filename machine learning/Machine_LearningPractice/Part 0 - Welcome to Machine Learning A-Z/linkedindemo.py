# -*- coding:utf-8 *-*
import sys
import requests
import re
import copy
import operator, pymongo
from bs4 import BeautifulSoup
import json, time, uuid
from datetime import datetime

reload(sys)
sys.setdefaultencoding("utf-8")
import urllib3
from urlparse import urljoin
import concurrent.futures

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
sys.path.insert(0, "..")
from loggingmodule import logger_module


class XcmmndrctinxScrape:
    """
    call crawl method to get list of all urls to be crawled from a source page
    call scrape_job_value to get desired crawled data from specified url
    """

    def __init__(self):
        self.date_field = datetime.now().strftime("%d:%m:%Y:%H:%M:%S")
        self.obj = logger_module.LogggerObj()
        self.logger = self.obj.logger_obj()
        self.lxml_data = []
        self.headers = {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; WOW64)\
                AppleWebKit/537.36 (KHTML'}

    def scrape_job_value(self, url_link, sourceurl):
        """
        :param url_link: url to be crawled
        :param refcode: reference
        :return: scraped data from the link provided
        """
        job_title = ""
        company_name = ""
        experience_required = ""
        job_location = ""
        job_description = ""
        key_skill = ""
        desired_condidate_profile = ""
        edu_requirement = ""
        about_company = ""
        raw_jd_disc = ""
        salary = ""
        industry = ""
        functional_area = ""
        role_category = ""
        role = ""
        job_type = ""
        category_name = ""
        min_experience = 0
        max_experience = 0
        min_salary = 0
        max_salary = 0
        job_posted_date = ""
        job_description_html = ""
        reference = ""
        cc = 0
        while cc < 10:
            cc += 1
            try:
                session = requests.session()
                # session.proxies = self.proxi_get(url_li)
                resp = session.get(url_link, headers=self.headers, timeout=180, verify=False)
                # print '+++++++', resp.status_code, url_link
                if resp.status_code == 200:
                    break
            except Exception as e:
                print e
                self.logger.error("xcmmndrctinxScrape error " + str(e) + ' crawl')
                time.sleep(5)
                pass
        if resp.status_code == 200:
            sou = BeautifulSoup(resp.text, 'lxml')
            raw_title = sou.find("h1", {"class": "general-pos ad-y-auto-txt"})
            if raw_title:
                job_title = raw_title.text.strip().encode("ascii",'ignore')
            raw_loc = sou.find("a", {"class": "loc-link"})
            # if raw_loc:
            #     job_location = raw_loc.text.strip().encode("utf8")+", hong kong"
            if job_location=="":
                job_location = "hong kong"
            raw_indus = sou.find("div", {"class": "primary-meta-box row meta-industry"})
            if raw_indus:
                if raw_indus.p:
                    industry = raw_indus.p.text.strip().encode("utf8")
            raw_sal = sou.find("div", {"class": "primary-meta-box row meta-salary"})
            if raw_sal:
                if raw_sal.p:
                    salary = raw_sal.p.text.strip().encode("utf8")
            raw_job_type = sou.find("div", {"class": "primary-meta-box row meta-employmenttype"})
            if raw_job_type:
                if raw_job_type.p:
                    job_type = raw_job_type.p.text.strip().encode("utf8")
            raw_about = sou.find("div", {"class": "primary-profile-detail"})
            if raw_about:
                about_company = ' '.join(raw_about.stripped_strings)
            raw_ref = sou.find("p", {"class": "data-ref ref-jobsdb"})
            if raw_ref:
                reference = ' '.join(raw_ref.stripped_strings).split("Ref.")[-1].strip()

            if "-" in salary:
                try:
                    min_max = salary.lower().replace(",","").replace("rm","").replace("s$","").replace("hk$","").split("-")
                    is_month = False
                    is_annum = False
                    if 'month' in salary.lower():
                        is_month = True
                    if 'annum' in salary.lower():
                        is_annum = True
                    # if 'lpa' not in salary.lower():
                    min_salary = min_max[0].replace(",", "").strip()
                    if len(min_salary.split()) == 2:
                        if is_month:
                            min_salary = int(float(min_salary.split()[1].strip()))*12
                        else:
                            min_salary = int(float(min_salary.split()[1].strip()))
                    if is_month:
                        min_salary = int(float(min_salary))*12
                        max_salary = int(float(min_max[1].strip().replace(",", "").split()[0].strip()))*12
                    else:
                        min_salary = int(float(min_salary))
                        max_salary = int(float(min_max[1].strip().replace(",", "").split()[0].strip()))
                    # else:
                    #     min_salary = int(min_max[0].replace(",", "").strip()) * 100000
                    #     max_salary = int(min_max[1].replace(",", "").split()[0].strip()) * 100000
                except Exception, e:
                    print e
                    min_salary = 0
                    max_salary = 0
                    pass
            raw_job_desc = sou.find("div", {"class": "jobad-primary-details col-xs-9"})
            if raw_job_desc:
                for kl in raw_job_desc.find_all("img"):
                    kl.decompose()
                for km in raw_job_desc.find_all("a"):
                    km.decompose()
                for tag in raw_job_desc():
                    for attribute in ["class", "id", "name", "style"]:
                        del tag[attribute]
                job_description = raw_job_desc.text.strip().encode('utf8')
                jobdesc = "<div><h3>Job Description :</h3><p></p>" + str(' '.join(str(raw_job_desc).split())).encode('ascii','ignore') + "</div>"
                job_description_html = ' '.join(re.sub("(<!--.*?-->)", "", jobdesc, flags=re.MULTILINE).split())

            # print salary

            exper = ""
            person_experience = re.findall(
                r'((\d+\.)?(\d+\s*?(-|–|to|\+)?\s*?(\d+)?\s*?(yrs|yr|years|year|YRS|YR|YEARS|YEAR|Years|Year)))',
                job_description)
            exper = ""
            # print person_experience
            if len(person_experience) > 0:
                exper = person_experience[0][0]
            experience_required = ' '.join(exper.split()).encode('ascii', 'ignore')
            if "+" in str(exper):
                sal_val = exper.split("+")
                try:
                    min_experience = float(sal_val[0])
                except:
                    pass
                try:
                    max_experience = min_experience + 3.0
                except:
                    pass
            elif "-" in str(exper) or "–" in str(exper):
                sal_val = exper.split("-")
                if len(sal_val) < 2:
                    sal_val = exper.split('–')
                try:
                    min_experience = float(sal_val[0])
                except:
                    pass
                try:
                    max_experience = float(sal_val[1].lower().split("y")[0].strip())
                except:
                    pass
            elif "to" in str(exper):
                sal_val = exper.split("to")
                try:
                    min_experience = float(sal_val[0])
                except:
                    pass
                try:
                    max_experience = float(sal_val[1].lower().split("y")[0].strip())
                except:
                    pass
            else:
                if exper:
                    try:
                        max_experience = exper.lower().split("y")[0]
                        if max_experience:
                            exp_max = max_experience.split()
                            if len(exp_max) == 2:
                                min_experience = float(exp_max[0])
                                max_experience = float(exp_max[1])
                            else:
                                min_experience = float(exp_max[0])
                                max_experience = float(exp_max[0]) + 3.0
                    except:
                        pass
            dic = {}
            if reference:
                dic["additional_info"] = {"reference": reference}
            dic["job_id"] = "scraped_" + str(uuid.uuid1())[1:8]
            dic["job_url"] = url_link
            dic["source_url"] = sourceurl
            dic["min_salary"] = min_salary
            dic["max_salary"] = max_salary
            dic["min_experience"] = min_experience
            dic["max_experience"] = max_experience
            dic["category"] = category_name
            dic["industry"] = industry
            dic["functional_area"] = functional_area
            dic["role_category"] = role_category
            dic["role"] = role
            dic["job_title"] = job_title.encode('utf8')
            dic["company_name"] = company_name
            dic["experience_required"] = experience_required
            dic["job_location"] = job_location
            dic["job_description"] = ' '.join(job_description.replace('\n', '').encode('utf8').split())
            dic["key_skill"] = key_skill
            dic["desired_condidate_profile"] = ' '.join(desired_condidate_profile.split())
            dic["edu_requirement"] = edu_requirement
            dic["about_company"] = ' '.join(about_company.split()).encode("ascii", "ignore")
            dic["salary"] = salary.encode("ascii", "ignore")
            dic["job_posted_date"] = job_posted_date
            dic["job_crawled_date"] = self.date_field
            dic["job_description_html"] = ' '.join(job_description_html.split()).replace('\n', '').replace("\xc2\xa0","").encode('ascii','ignore')
            dic["job_type"] = job_type
            if dic["job_title"] != "" and dic["job_description"] != "":
                return dic
            else:
                return None

    def crawl(self, url_list):
        """
        :param url_list: source of main url(s)
        :return: lxml of all links to be crawled
        """
        All_urls = []
        for url_li in url_list:
            is_pagination = True
            i = 1
            while is_pagination:
                url = url_li+str(i)
                # print url
                cc = 0
                while cc < 1:
                    cc += 1
                    try:
                        session = requests.session()
                        # session.proxies = self.proxi_get(url_li)
                        res = session.get(url, headers=self.headers, timeout=180, verify=False)
                        print '+++++++', res.status_code, url
                        if res.status_code == 200:
                            break
                    except Exception as e:
                        print e
                        self.logger.error("xcmmndrctinxScrape error " + str(e) + ' crawl')
                        time.sleep(5)
                        pass
                if res.status_code == 200:
                    i += 1
                    soup = BeautifulSoup(res.text, 'lxml')
                    raw_job_div = soup.find("div",{"data-automation":"jobListing"})
                    if raw_job_div:
                        rawdiv = raw_job_div.find_all("div", {"class": "NqLrol7"})
                        if len(rawdiv)>0:
                            for jobli in rawdiv:
                                raw_link = jobli.find("a",{"rel":"noopener noreferrer"})
                                if raw_link:
                                    raw_li = urljoin(urljoin(url,"/"),raw_link.get("href"))
                                    All_urls.append(raw_li)
                        else:
                            is_pagination = False
                    else:
                        is_pagination = False
                else:
                    is_pagination = False
        Total_data = []
        print "Total urls", len(list(set(All_urls)))
        # All_urls = ['https://hk.jobsdb.com/hk/en/job/digital-system-analyst-in-a-multinational-luxury-retailer-100003006594837']
        # All_urls = ["https://hk.jobsdb.com/hk/en/job/it-manager-senior-manager-big-data-analytics-100003006594681"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            future_to_url = {executor.submit(self.scrape_job_value, url_jb, url_list[0]): url_jb
                             for url_jb in
                             list(set(All_urls))[:]}
            for future in concurrent.futures.as_completed(future_to_url):
                url2 = future_to_url[future]
                try:
                    data = future.result()
                    if data:
                        Total_data.append(data)
                except Exception as exc:
                    print '=====>', exc, url2
                    self.logger.error("xcmmndrctinxScrape" + str(exc) + " scrape_job_value")
                    pass
        return Total_data


if __name__ == "__main__":
    ob = XcmmndrctinxScrape()
    main_url = ["https://in.linkedin.com/jobs/t-a-solutions-jobs"
        ]
    # main_url = ["https://www.ambition.com.my/jobs/?commit=&page="]
    crawled_jobs_array = ob.crawl(main_url[:])
    print  len(crawled_jobs_array)#,crawled_jobs_array[0]["job_description_html"]
    # f = open("xamb.txt","wb")
    # f.write(str(crawled_jobs_array))
    # f.close()






