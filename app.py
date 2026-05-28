"""
app.py  —  Parallel Scraper Agent
All 175 scrapers consolidated into one single file.

Usage:
    python app.py                          # run all tasks (20 workers default)
    python app.py --workers 10             # limit concurrency
    python app.py --filter FBI             # only tasks matching substring
    python app.py --task BD_POLICE_WANTED  # run a single named task
    python app.py --list                   # list all task names and exit
"""

import argparse
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Colours ─────────────────────────────────────────────────────────────────
COLORS = {
    "AR_MOJ_RePET_E": "\033[94m",
    "AR_MOJ_RePET_P": "\033[92m",
    "AR_NIS_CPL": "\033[93m",
    "BD_POLICE_WANTED": "\033[91m",
    "BN_BDCB_AL": "\033[96m",
    "CA_GC_NRO": "\033[95m",
    "CA_SPEC_MEAS_ESV": "\033[33m",
    "CA_SPEC_MEAS_GUATEMALA": "\033[32m",
    "CA_SPEC_MEAS_HAMAS_TA": "\033[34m",
    "CA_SPEC_MEAS_MOLDOVA": "\033[35m",
    "CA_SPEC_MEAS_SRILANKA": "\033[36m",
    "CA_SPEC_MEAS_SUDAN": "\033[97m",
    "CH_UN_COTE_DIVOIRE_1572": "\033[94m",
    "CI_ADB_DEBARRED_ENTITIES": "\033[92m",
    "CT_DOL_DL": "\033[93m",
    "CT_DSS_AAL": "\033[91m",
    "DK_DIS_ENTRY_BAN": "\033[96m",
    "DK_IS_PROHIBITED_ENTITIES": "\033[95m",
    "DK_IS_PROHIBITED_PERSONS": "\033[33m",
    "DOC_BIS_RL": "\033[32m",
    "DOS_ADP": "\033[34m",
    "DOS_CRL": "\033[35m",
    "DOS_SDP": "\033[36m",
    "DOS_TEL": "\033[97m",
    "EE_FSA_IA": "\033[94m",
    "EE_MFA_BELARUS": "\033[92m",
    "EE_MFA_HR": "\033[93m",
    "EE_MFA_RUSSIA": "\033[91m",
    "ES_BOS_SANCTIONS": "\033[96m",
    "ES_NPC_TERRORISTS": "\033[95m",
    "ES_WA_CNMV": "\033[33m",
    "EU_EBRD_INELIGIBLE_ENTITIES": "\033[32m",
    "EU_ECB_SANCTIONS": "\033[34m",
    "EU_EC_EDES": "\033[35m",
    "EU_EIB_LOE": "\033[36m",
    "EU_EUROPOL_MWF": "\033[97m",
    "FBI_LAW_ENFORCEMENT": "\033[94m",
    "FBI_MWT": "\033[92m",
    "FBI_SI": "\033[93m",
    "FBI_TOP_10": "\033[91m",
    "GB_FCA_UCB": "\033[96m",
    "GB_OFSI_TR_MP": "\033[95m",
    "GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST": "\033[33m",
    "GG_GFSC_DD": "\033[32m",
    "HK_ICAC_WL": "\033[34m",
    "HU_MNB_MSW": "\033[35m",
    "IE_CBI_PN": "\033[36m",
    "IL_IIPB_PIL": "\033[97m",
    "IL_MOF_WMD": "\033[94m",
    "IL_MOJ_TERR_ORG": "\033[92m",
    "IM_FSA_DD": "\033[93m",
    "IM_FSA_ENFORCEMENT": "\033[91m",
    "INTERPOL_UN_Notices_Entities": "\033[96m",
    "INTERPOL_UN_Notices_Individuals": "\033[95m",
    "IN_BANNED_ORG": "\033[33m",
    "IN_SEBI_DEBARRED_ENTITIES": "\033[32m",
    "IN_UNLAWFUL_ASSOCIATION": "\033[34m",
    "JP_METI": "\033[35m",
    "JP_UN_CAR_2134": "\033[36m",
    "JP_UN_CONGO": "\033[97m",
    "JP_UN_SANCTIONS": "\033[94m",
    "JP_UN_SANCTIONS_IRAN": "\033[92m",
    "JP_UN_SANCTIONS_SOMALIA": "\033[93m",
    "JP_UN_SANCTIONS_SYRIA": "\033[91m",
    "JP_UN_SS_2206": "\033[96m",
    "JP_UN_YE_2140": "\033[95m",
    "KE_FRC_DTFS": "\033[33m",
    "KG_FIU_SANCTIONS_ENTITIES": "\033[32m",
    "KG_FIU_SANCTIONS_INDIVIDUALS": "\033[34m",
    "KR_MFA_SAKP": "\033[35m",
    "LB_ISF_NTFL": "\033[36m",
    "LV_FIU_SANCTIONED_SUBJECTS": "\033[97m",
    "MC_BT_NAFL": "\033[94m",
    "MN_OSP_SDVR": "\033[92m",
    "MT_MPF_WANTED": "\033[93m",
    "MY_BNM_WANTED": "\033[91m",
    "MY_BNM_WARNING_LETTERS": "\033[96m",
    "MY_MHA_MOHA_LIST": "\033[95m",
    "MY_SCM_IA": "\033[33m",
    "MY_SCM_WANTED": "\033[32m",
    "NG_EFCC_WANTED_PERSON": "\033[34m",
    "NG_NIGSAC_ENTITIES": "\033[35m",
    "NG_NIGSAC_INDIVIDUALS": "\033[36m",
    "NO_BLACK_LIST": "\033[97m",
    "NY_OGS_IDA": "\033[94m",
    "OIG_MOST_WANTED": "\033[92m",
    "OM_NCTC_LL": "\033[93m",
    "PA_TREAS_SCI": "\033[91m",
    "PE_MEF_DISQUALIFIED_SUPPLIERS": "\033[96m",
    "PL_MOF_SRM": "\033[95m",
    "PRM_AK_DHSS_EPL": "\033[33m",
    "PRM_AL_AMA_SP": "\033[32m",
    "PRM_AR_DHS_EP": "\033[34m",
    "PRM_CA_DHCS_SIPL": "\033[35m",
    "PRM_CA_DIR_DC": "\033[36m",
    "PRM_DC_DDS_PSL": "\033[97m",
    "scrape_table_to_text": "\033[94m",
    "PRM_GA_DCH_EIE": "\033[92m",
    "PRM_HI_DHS_PERL": "\033[93m",
    "PRM_IA_DHS_MPSL": "\033[91m",
    "PRM_ID_DHW_MPEL": "\033[96m",
    "PRM_IL_DHFS_PSL": "\033[95m",
    "PRM_IN_FSSA_PT": "\033[33m",
    "PRM_KS_DHE_TPL": "\033[32m",
    "PRM_KY_CHFS_PT": "\033[34m",
    "PRM_LA_DH_AAL": "\033[35m",
    "PRM_LA_DOA_DV": "\033[36m",
    "PRM_MA_MH_SEMP": "\033[97m",
    "PRM_MD_BPW_CB": "\033[94m",
    "PRM_MD_MDH_MSPL": "\033[92m",
    "PRM_MI_DHHS_SPL": "\033[93m",
    "PRM_MO_DSS_PS": "\033[91m",
    "PRM_MS_DM_SPL": "\033[96m",
    "PRM_MT_DPHHS_TMP": "\033[95m",
    "PRM_NC_DA_DV": "\033[33m",
    "PRM_NC_DHHS_EPL": "\033[32m",
    "PRM_ND_DHS_MPEL": "\033[34m",
    "PRM_NH_DHHS_MPESL": "\033[35m",
    "PRM_NJ_MFD_PER": "\033[36m",
    "PRM_NV_DHHS_MSL": "\033[97m",
    "PRM_NV_OLC_DC": "\033[94m",
    "PRM_NY_DOL_DL": "\033[92m",
    "PRM_OH_DM_MPESL": "\033[93m",
    "PRM_OR_BOLI_IC": "\033[91m",
    "PRM_PA_DGS_DSL": "\033[96m",
    "PRM_SC_DHHS_EPL": "\033[95m",
    "PRM_SC_SFAA_SD": "\033[33m",
    "PRM_TN_OPI_TPL": "\033[32m",
    "PRM_TX_HHS_EP": "\033[34m",
    "PRM_TX_TC_DVL": "\033[35m",
    "PRM_WA_DOLI_DC": "\033[36m",
    "PRM_WA_HCA_PTEL": "\033[97m",
    "PRM_WI_DOT_DSIC": "\033[94m",
    "PRM_WV_DHHR_PSE": "\033[92m",
    "PRM_WV_WVPD_DV": "\033[93m",
    "PRM_WY_DH_PEL": "\033[91m",
    "QA_MOI_SANCTIONS_LIST": "\033[96m",
    "SECO_SWISS_GUATEMALA": "\033[95m",
    "SECO_SWISS_HAMAS": "\033[33m",
    "SECO_SWISS_MOLDOVA": "\033[32m",
    "SG_ACRA_SUSPENSION": "\033[34m",
    "SG_MAS_RUSSIA": "\033[35m",
    "SG_SGX_WATCH_LIST": "\033[36m",
    "TH_AMLO_SANCTIONS": "\033[97m",
    "TREAS_FINCEN_ADVISORY": "\033[94m",
    "TREAS_FINCEN_PMLC": "\033[92m",
    "TR_MTF_ARTICLE_3": "\033[93m",
    "TR_MTF_ARTICLE_5": "\033[91m",
    "TR_MTF_ARTICLE_6": "\033[96m",
    "TR_MTF_ARTICLE_7": "\033[95m",
    "TT_FIU_DPRK": "\033[33m",
    "TT_FIU_HAITI": "\033[32m",
    "TW_CBI_MW": "\033[34m",
    "TW_FSC_EA": "\033[35m",
    "UA_SSU_WANTED": "\033[36m",
    "US_AF_FUGITIVES": "\033[97m",
    "US_CBP_UEL": "\033[94m",
    "get_next_button": "\033[92m",
    "US_DEA_MOST_WANTED": "\033[93m",
    "US_DHS_BFV": "\033[91m",
    "US_DOT_MOST_WANTED": "\033[96m",
    "US_EPA_FUGITIVES": "\033[95m",
    "US_FDA_CI": "\033[33m",
    "US_FDA_WL": "\033[32m",
    "US_FD_OCI_MOST_WANTED_FUGITIVES": "\033[34m",
    "US_NCDST_IFD_LIST": "\033[35m",
    "US_NJL_IDA": "\033[36m",
    "US_OGT_SCI": "\033[97m",
    "US_PDGS_IFP_LIST": "\033[94m",
    "scroll_page": "\033[92m",
    "US_SEC_LR": "\033[93m",
    "US_SS_MWF": "\033[91m",
    "ZA_FIC_SANCTIONS": "\033[96m",
    "ZA_FIC_TFS": "\033[95m",
    "ZA_FSCA_EA": "\033[33m",
    "RESET": "\033[0m",
    "BOLD":  "\033[1m",
    "RED":   "\033[91m",
    "CYAN":  "\033[96m",
}

# ── Logging ───────────────────────────────────────────────────────────────────
print_lock = threading.Lock()

def log(task_id, msg, level="info"):
    color  = COLORS.get(task_id, "")
    prefix = {"info": "\u2139", "success": "\u2714", "error": "\u2718", "warn": "\u26a0"}.get(level, "\u2022")
    ts     = datetime.now().strftime("%H:%M:%S")
    with print_lock:
        print(f"{color}{COLORS['BOLD']}[{task_id}]{COLORS['RESET']} {color}[{ts}] {prefix}  {msg}{COLORS['RESET']}", flush=True)

# ── Scrapers ─────────────────────────────────────────────────────────────────

# ────────────────────────────────────────────────────────────────────
def AR_MOJ_RePET_E():
    task_id = "AR_MOJ_RePET_E"
    log(task_id, "Starting AR_MOJ_RePET_E …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def AR_MOJ_RePET_E_inner():
            page = "https://repet.jus.gob.ar/#personas"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/section/div/div/div[6]/div/div/section/div/div[2]').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = AR_MOJ_RePET_E_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "AR_MOJ_RePET_E", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "AR_MOJ_RePET_E", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def AR_MOJ_RePET_P():
    task_id = "AR_MOJ_RePET_P"
    log(task_id, "Starting AR_MOJ_RePET_P …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def AR_MOJ_RePET_P_inner():
            page = "https://repet.jus.gob.ar/#personas"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/section/div/div/div[6]/div/div/section/div/div[1]').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = AR_MOJ_RePET_P_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "AR_MOJ_RePET_P", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "AR_MOJ_RePET_P", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def AR_NIS_CPL():
    task_id = "AR_NIS_CPL"
    log(task_id, "Starting AR_NIS_CPL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def AR_NIS_CPL_inner():
            page = "https://www.argentina.gob.ar/superintendencia-de-seguros/mercado-asegurador/compa%C3%B1ias-seguro/iquidacion"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/section[2]').text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = AR_NIS_CPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "AR_NIS_CPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "AR_NIS_CPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def BD_POLICE_WANTED():
    task_id = "BD_POLICE_WANTED"
    log(task_id, "Starting BD_POLICE_WANTED …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def BD_POLICE_WANTED_inner():
            page = "https://www.police.gov.bd/en/wanted_person?page=1"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            all_names = []
            pagination_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'wanted_person?page=')]")
            page_nums = []
            for link in pagination_links:
                href = link.get_attribute("href")
                try:
                    num = int(href.split("page=")[-1])
                    page_nums.append(num)
                except ValueError:
                    pass
            total_pages = max(page_nums) if page_nums else 1
            print(f"Total pages detected: {total_pages}")
            for page_num in range(1, total_pages + 1):
                if page_num > 1:
                    driver.get(f"https://www.police.gov.bd/en/wanted_person?page={page_num}")
                    time.sleep(5)
                print(f"\n--- Scraping Page {page_num} of {total_pages} ---")
                rows = driver.find_elements(By.XPATH, "//table//tr[td]")
                print(f"  Found {len(rows)} record(s)")
                for row in rows:
                    try:
                        # Name is the first line of text in the first <td>
                        cell_text = row.find_element(By.XPATH, "./td[1]").text.strip()
                        name = cell_text.splitlines()[0].strip()
                        if name:
                            all_names.append(name)
                            print(f"  + {name}")
                    except Exception as e:
                        print(f"  ERROR reading row: {e}")
            driver.quit()
            Data = "\n".join(all_names)
            print(f"\n{'='*50}")
            print(f"Total wanted persons: {len(all_names)}")
            print(f"{'='*50}")
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = BD_POLICE_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "BD_POLICE_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "BD_POLICE_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def BN_BDCB_AL():
    task_id = "BN_BDCB_AL"
    log(task_id, "Starting BN_BDCB_AL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def BN_BDCB_AL_inner():
            page = "https://www.bdcb.gov.bn/consumer/bdcb-alert-list"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//*[contains(text(),'Date added')]//ancestor::p").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = BN_BDCB_AL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "BN_BDCB_AL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "BN_BDCB_AL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_GC_NRO():
    task_id = "CA_GC_NRO"
    log(task_id, "Starting CA_GC_NRO …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_GC_NRO_inner():
            page = "https://science.gc.ca/site/science/en/safeguarding-your-research/guidelines-and-tools-implement-research-security/sensitive-technology-research-and-affiliations-concern/named-research-organizations#top"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//*[contains(text(),'Named Research Organizations List')]//following::div").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CA_GC_NRO_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_GC_NRO", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_GC_NRO", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_SPEC_MEAS_ESV():
    task_id = "CA_SPEC_MEAS_ESV"
    log(task_id, "Starting CA_SPEC_MEAS_ESV …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_SPEC_MEAS_ESV_inner():
            page = "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/esv-vec.aspx?lang=eng"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//h2[contains(text(),'Selected documents')]//following::div[2]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CA_SPEC_MEAS_ESV_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_SPEC_MEAS_ESV", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_SPEC_MEAS_ESV", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_SPEC_MEAS_GUATEMALA():
    task_id = "CA_SPEC_MEAS_GUATEMALA"
    log(task_id, "Starting CA_SPEC_MEAS_GUATEMALA …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_SPEC_MEAS_GUATEMALA_inner():
            page = "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/guatemala.aspx?lang=eng"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//h2[contains(text(),'Selected documents')]//following::div[2]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CA_SPEC_MEAS_GUATEMALA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_SPEC_MEAS_GUATEMALA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_SPEC_MEAS_GUATEMALA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_SPEC_MEAS_HAMAS_TA():
    task_id = "CA_SPEC_MEAS_HAMAS_TA"
    log(task_id, "Starting CA_SPEC_MEAS_HAMAS_TA …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_SPEC_MEAS_HAMAS_TA_inner():
            page = "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/terrorists-terroristes.aspx?lang=eng"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//h2[contains(text(),'Selected documents')]//following::div[2]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CA_SPEC_MEAS_HAMAS_TA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_SPEC_MEAS_HAMAS_TA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_SPEC_MEAS_HAMAS_TA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_SPEC_MEAS_MOLDOVA():
    task_id = "CA_SPEC_MEAS_MOLDOVA"
    log(task_id, "Starting CA_SPEC_MEAS_MOLDOVA …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_SPEC_MEAS_MOLDOVA_inner():
            page = "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/moldova.aspx?lang=eng"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//h2[contains(text(),'Selected documents')]//following::div").text
            print(Link)
            driver.close()
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = CA_SPEC_MEAS_MOLDOVA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_SPEC_MEAS_MOLDOVA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_SPEC_MEAS_MOLDOVA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_SPEC_MEAS_SRILANKA():
    task_id = "CA_SPEC_MEAS_SRILANKA"
    log(task_id, "Starting CA_SPEC_MEAS_SRILANKA …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_SPEC_MEAS_SRILANKA_inner():
            page = "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/sri_lanka.aspx?lang=eng"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data= driver.find_element(By.XPATH, "//h2[contains(text(),'Selected documents')]/following::div").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CA_SPEC_MEAS_SRILANKA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_SPEC_MEAS_SRILANKA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_SPEC_MEAS_SRILANKA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CA_SPEC_MEAS_SUDAN():
    task_id = "CA_SPEC_MEAS_SUDAN"
    log(task_id, "Starting CA_SPEC_MEAS_SUDAN …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CA_SPEC_MEAS_SUDAN_inner():
            page = "https://www.international.gc.ca/world-monde/international_relations-relations_internationales/sanctions/sudan-soudan.aspx?lang=eng"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//h2[contains(text(),'Selected documents')]//following::div[2]").text
            print(Date)
            driver.quit()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = CA_SPEC_MEAS_SUDAN_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CA_SPEC_MEAS_SUDAN", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CA_SPEC_MEAS_SUDAN", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CH_UN_COTE_DIVOIRE_1572():
    task_id = "CH_UN_COTE_DIVOIRE_1572"
    log(task_id, "Starting CH_UN_COTE_DIVOIRE_1572 …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CH_UN_COTE_DIVOIRE_1572_inner():
            page = "https://www.seco.admin.ch/seco/de/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos/sanktionsmassnahmen/massnahmen-gegenueber-cote-d-ivoire.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Änderungen XML')]//following::td[3]").text
            print(Link)
            driver.close()
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = CH_UN_COTE_DIVOIRE_1572_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CH_UN_COTE_DIVOIRE_1572", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CH_UN_COTE_DIVOIRE_1572", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CI_ADB_DEBARRED_ENTITIES():
    task_id = "CI_ADB_DEBARRED_ENTITIES"
    log(task_id, "Starting CI_ADB_DEBARRED_ENTITIES …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def CI_ADB_DEBARRED_ENTITIES_inner():
            page = "https://www.afdb.org/en/projects-operations/debarment-and-sanctions-procedures"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'List of Debarred Entities')]//following::a[2]").get_attribute("href")
            print(Link)
            r = requests.get(Link)
            r.raise_for_status()
            df = pd.read_csv(io.BytesIO(r.content), skiprows=1)
            Data = df.to_csv(index=False)
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CI_ADB_DEBARRED_ENTITIES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CI_ADB_DEBARRED_ENTITIES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CI_ADB_DEBARRED_ENTITIES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CT_DOL_DL():
    task_id = "CT_DOL_DL"
    log(task_id, "Starting CT_DOL_DL …")
    try:
        import hashlib
        import time
        import requests
        import io
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def CT_DOL_DL_inner():
            website = 'https://portal.ct.gov/dol/knowledge-base/articles/wage-and-workplace-standards/debarment-lists?language=en_US'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            time.sleep(5)
            Content = driver.find_element(By.XPATH, "//strong[contains(text(),'Debarment Lists')]/following::a[2]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CT_DOL_DL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CT_DOL_DL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CT_DOL_DL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def CT_DSS_AAL():
    task_id = "CT_DSS_AAL"
    log(task_id, "Starting CT_DSS_AAL …")
    try:
        import hashlib
        import time
        import requests
        import io
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def CT_DSS_AAL_inner():
            website = 'https://portal.ct.gov/DSS/Quality-Assurance/Quality-Assurance-Administrative-Actions-List?language=en_US'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            time.sleep(5)
            driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "/html/body/div/center/table").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = CT_DSS_AAL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "CT_DSS_AAL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "CT_DSS_AAL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DK_DIS_ENTRY_BAN():
    task_id = "DK_DIS_ENTRY_BAN"
    log(task_id, "Starting DK_DIS_ENTRY_BAN …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def DK_DIS_ENTRY_BAN_inner():
            page = "https://nyidanmark.dk/da/Ord-og-begreber/US/Religi%C3%B8se-forkyndere/Religi%C3%B8se-forkyndere-med-indrejseforbud/?anchor=7C5D2D143D284E4EB2829BA5F0F04837&callbackItem=C0848E0180C34017BFB14DC9BC116572&callbackAnchor=608DF21DB20C40B68646A6B6804E595D7C5D2D143D284E4EB2829BA5F0F04837"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="main-wrapper"]/div[3]/main/div[2]/div/section[1]/article').text
            print(Link)
            driver.close()
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = DK_DIS_ENTRY_BAN_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DK_DIS_ENTRY_BAN", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DK_DIS_ENTRY_BAN", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DK_IS_PROHIBITED_ENTITIES():
    task_id = "DK_IS_PROHIBITED_ENTITIES"
    log(task_id, "Starting DK_IS_PROHIBITED_ENTITIES …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def DK_IS_PROHIBITED_ENTITIES_inner():
            page = "https://us.dk/center-for-dokumentation-og-indsats-mod-ekstremisme/indhentning-og-analyse/forbudslisten/selve-forbudslisten/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Juridiske')]//following::table").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = DK_IS_PROHIBITED_ENTITIES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DK_IS_PROHIBITED_ENTITIES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DK_IS_PROHIBITED_ENTITIES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DK_IS_PROHIBITED_PERSONS():
    task_id = "DK_IS_PROHIBITED_PERSONS"
    log(task_id, "Starting DK_IS_PROHIBITED_PERSONS …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def DK_IS_PROHIBITED_PERSONS_inner():
            page = "https://us.dk/center-for-dokumentation-og-indsats-mod-ekstremisme/indhentning-og-analyse/forbudslisten/selve-forbudslisten/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Juridiske')]//following::table[2]").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = DK_IS_PROHIBITED_PERSONS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DK_IS_PROHIBITED_PERSONS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DK_IS_PROHIBITED_PERSONS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DOC_BIS_RL():
    task_id = "DOC_BIS_RL"
    log(task_id, "Starting DOC_BIS_RL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def DOC_BIS_RL_inner():
            page = "https://www.bis.gov/OAC"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Requester List (CSV)')]").get_attribute("href")
            print(Link)
            r = requests.get(Link)
            r.raise_for_status()
            df = pd.read_csv(io.BytesIO(r.content), skiprows=1)
            Data = df.to_csv(index=False)
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = DOC_BIS_RL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DOC_BIS_RL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DOC_BIS_RL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DOS_ADP():
    task_id = "DOS_ADP"
    log(task_id, "Starting DOS_ADP …")
    try:
        import hashlib
        import time
        import requests
        import io
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def DOS_ADP_inner():
            website = 'https://www.pmddtc.state.gov/ddtc_public?id=ddtc_kb_article_page&sys_id=8a89528adb3cd30044f9ff621f961931'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//h1[contains(text(),'Administratively Debarred Parties')]/following::table").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = DOS_ADP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DOS_ADP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DOS_ADP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DOS_CRL():
    task_id = "DOS_CRL"
    log(task_id, "Starting DOS_CRL …")
    try:
        import hashlib
        import time
        import requests
        import io
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def DOS_CRL_inner():
            website = 'https://www.state.gov/division-for-counter-threat-finance-and-sanctions/cuba-restricted-list'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'LIST OF RESTRICTED ENTITIES')]/ancestor::div[1]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = DOS_CRL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DOS_CRL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DOS_CRL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DOS_SDP():
    task_id = "DOS_SDP"
    log(task_id, "Starting DOS_SDP …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def DOS_SDP_inner():
            website = 'https://www.pmddtc.state.gov/ddtc_public?id=ddtc_kb_article_page&sys_id=7188dac6db3cd30044f9ff621f961914'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Date1= driver.find_element(By.XPATH, "//h1[contains(text(),'Statutorily Debarred Parties')]/following::div[contains(text(),'Notice Date')]").click()
            time.sleep(2)
            Date2 = driver.find_element(By.XPATH, "//h1[contains(text(),'Statutorily Debarred Parties')]/following::div[contains(text(),'Notice Date')]").click()
            time.sleep(2)
            Content = driver.find_element(By.XPATH, "//h1[contains(text(),'Statutorily Debarred Parties')]/following::div[contains(text(),'Notice Date')]/following::td[4]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = DOS_SDP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DOS_SDP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DOS_SDP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def DOS_TEL():
    task_id = "DOS_TEL"
    log(task_id, "Starting DOS_TEL …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def DOS_TEL_inner():
            website = 'https://www.state.gov/terrorist-exclusion-list/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//h2[contains(text(),'Designation Criteria')]//ancestor::div[@class='entry-content']").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = DOS_TEL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "DOS_TEL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "DOS_TEL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EE_FSA_IA():
    task_id = "EE_FSA_IA"
    log(task_id, "Starting EE_FSA_IA …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def EE_FSA_IA_inner():
            website = 'https://www.fi.ee/en/alerts'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//h1[contains(text(),'Investor alerts')]//following::div[@class='views-row'][1]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = EE_FSA_IA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EE_FSA_IA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EE_FSA_IA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EE_MFA_BELARUS():
    task_id = "EE_MFA_BELARUS"
    log(task_id, "Starting EE_MFA_BELARUS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def EE_MFA_BELARUS_inner():
            page = "https://www.vm.ee/en/sanctions-government-republic-view-situation-belarus"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            Link2 = driver.find_element(By.XPATH, '//*[@id="block-mainpagecontent"]/article/div[1]/div/div/div').text
            print(Link2)
            output_date = hashlib.sha256(Link2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = EE_MFA_BELARUS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EE_MFA_BELARUS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EE_MFA_BELARUS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EE_MFA_HR():
    task_id = "EE_MFA_HR"
    log(task_id, "Starting EE_MFA_HR …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def EE_MFA_HR_inner():
            page = "https://www.vm.ee/en/list-subjects-sanction-government-republic-ensure-following-human-rights"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            Link2 = driver.find_element(By.XPATH, '//*[@id="block-mainpagecontent"]/article/div[1]/div/div/div').text
            print(Link2)
            output_date = hashlib.sha256(Link2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = EE_MFA_HR_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EE_MFA_HR", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EE_MFA_HR", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EE_MFA_RUSSIA():
    task_id = "EE_MFA_RUSSIA"
    log(task_id, "Starting EE_MFA_RUSSIA …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def EE_MFA_RUSSIA_inner():
            page = "https://www.vm.ee/subjektide-nimekiri-vabariigi-valitsuse-sanktsioon-eesti-julgeoleku-ja-huvide-kaitseks-ning-venemaa"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.maximize_window()
            driver.get(page)
            Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Luban kõik')]").click()
            time.sleep(5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            Data = driver.find_element(By.XPATH, '//*[@id="block-mainpagecontent"]/article/div/div/div/div').text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = EE_MFA_RUSSIA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EE_MFA_RUSSIA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EE_MFA_RUSSIA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def ES_BOS_SANCTIONS():
    task_id = "ES_BOS_SANCTIONS"
    log(task_id, "Starting ES_BOS_SANCTIONS …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def ES_BOS_SANCTIONS_inner():
            page = "https://www.bde.es/wbe/en/punto-informacion/contenidos/sanciones-impuestas-banco-espana/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(10)
            #iframe = driver.find_element(By.XPATH, '//*[@id="myFrame"]')
            #driver.switch_to.frame(iframe)
            Data = driver.find_element(By.XPATH, "//strong[contains(text(),'Publication date:')]//ancestor::p").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = ES_BOS_SANCTIONS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "ES_BOS_SANCTIONS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "ES_BOS_SANCTIONS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def ES_NPC_TERRORISTS():
    task_id = "ES_NPC_TERRORISTS"
    log(task_id, "Starting ES_NPC_TERRORISTS …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def ES_NPC_TERRORISTS_inner():
            website = 'https://www.policia.es/_es/colabora_masbuscados.php'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'más buscados')]/following::div[@class='container-fluid bg-primary']").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = ES_NPC_TERRORISTS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "ES_NPC_TERRORISTS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "ES_NPC_TERRORISTS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def ES_WA_CNMV():
    task_id = "ES_WA_CNMV"
    log(task_id, "Starting ES_WA_CNMV …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def ES_WA_CNMV_inner():
            page = "https://www.cnmv.es/portal/ResultadoBusqueda?tipo=1"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Search for CNMV´s public warnings')]//ancestor::table").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = ES_WA_CNMV_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "ES_WA_CNMV", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "ES_WA_CNMV", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EU_EBRD_INELIGIBLE_ENTITIES():
    task_id = "EU_EBRD_INELIGIBLE_ENTITIES"
    log(task_id, "Starting EU_EBRD_INELIGIBLE_ENTITIES …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def EU_EBRD_INELIGIBLE_ENTITIES_inner():
            website = 'https://www.ebrd.com/home/who-we-are/strategies-governance-compliance/ebrd-sanctions-system/ineligible-entities.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//h3[contains(text(),'Ineligible Entities')]//following::h4").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = EU_EBRD_INELIGIBLE_ENTITIES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EU_EBRD_INELIGIBLE_ENTITIES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EU_EBRD_INELIGIBLE_ENTITIES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EU_ECB_SANCTIONS():
    task_id = "EU_ECB_SANCTIONS"
    log(task_id, "Starting EU_ECB_SANCTIONS …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def EU_ECB_SANCTIONS_inner():
            page = "https://www.ecb.europa.eu/ecb/sanctions/html/index.en.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//main").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = EU_ECB_SANCTIONS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EU_ECB_SANCTIONS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EU_ECB_SANCTIONS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EU_EC_EDES():
    task_id = "EU_EC_EDES"
    log(task_id, "Starting EU_EC_EDES …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def EU_EC_EDES_inner():
            website = 'https://ec.europa.eu/info/strategy/eu-budget/how-it-works/annual-lifecycle/implementation/anti-fraud-measures/edes/database_en'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(10)
            #driver.switch_to.frame(driver.find_element(By.XPATH, '/html/body/div[1]/div[3]/article/section/div/div[4]/div/div/iframe'))
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Country Code')]/following::tbody[2]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = EU_EC_EDES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EU_EC_EDES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EU_EC_EDES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EU_EIB_LOE():
    task_id = "EU_EIB_LOE"
    log(task_id, "Starting EU_EIB_LOE …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def EU_EIB_LOE_inner():
            page = "https://www.eib.org/en/about/accountability/anti-fraud/exclusion/index.htm"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//table").text
            print(Date)
            driver.close()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = EU_EIB_LOE_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EU_EIB_LOE", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EU_EIB_LOE", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def EU_EUROPOL_MWF():
    task_id = "EU_EUROPOL_MWF"
    log(task_id, "Starting EU_EUROPOL_MWF …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def EU_EUROPOL_MWF_inner():
            website = 'https://eumostwanted.eu/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Recently added or updated')]/ancestor::div[1]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = EU_EUROPOL_MWF_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "EU_EUROPOL_MWF", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "EU_EUROPOL_MWF", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def FBI_LAW_ENFORCEMENT():
    task_id = "FBI_LAW_ENFORCEMENT"
    log(task_id, "Starting FBI_LAW_ENFORCEMENT …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def FBI_LAW_ENFORCEMENT_inner():
            page = "https://www.fbi.gov/wanted/law-enforcement-assistance"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//h2[contains(text(),'Law Enforcement Assistance')]//following::ul").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = FBI_LAW_ENFORCEMENT_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "FBI_LAW_ENFORCEMENT", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "FBI_LAW_ENFORCEMENT", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def FBI_MWT():
    task_id = "FBI_MWT"
    log(task_id, "Starting FBI_MWT …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def FBI_MWT_inner():
            page = "https://www.fbi.gov/wanted/wanted_terrorists"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="query-results-dcb5bab4b980426bb0e401695ce2ee95"]/ul').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = FBI_MWT_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "FBI_MWT", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "FBI_MWT", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def FBI_SI():
    task_id = "FBI_SI"
    log(task_id, "Starting FBI_SI …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def FBI_SI_inner():
            page = "https://www.fbi.gov/wanted/terrorinfo"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="query-results-querylisting-1"]/ul').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = FBI_SI_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "FBI_SI", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "FBI_SI", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def FBI_TOP_10():
    task_id = "FBI_TOP_10"
    log(task_id, "Starting FBI_TOP_10 …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def FBI_TOP_10_inner():
            page = "https://www.fbi.gov/wanted/topten"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="query-results-0f737222c5054a81a120bce207b0446a"]/ul').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = FBI_TOP_10_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "FBI_TOP_10", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "FBI_TOP_10", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def GB_FCA_UCB():
    task_id = "GB_FCA_UCB"
    log(task_id, "Starting GB_FCA_UCB …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def GB_FCA_UCB_inner():
            page = "https://register.fca.org.uk/s/search?predefined=U"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//h1[contains(text(),'Unregistered Cryptoasset Businesses')]//following::table").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = GB_FCA_UCB_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "GB_FCA_UCB", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "GB_FCA_UCB", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def GB_OFSI_TR_MP():
    task_id = "GB_OFSI_TR_MP"
    log(task_id, "Starting GB_OFSI_TR_MP …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def GB_OFSI_TR_MP_inner():
            website = 'https://www.gov.uk/government/collections/enforcement-of-financial-sanctions#full-publication-update-history'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Last updated')]//following::dd").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = GB_OFSI_TR_MP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "GB_OFSI_TR_MP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "GB_OFSI_TR_MP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST():
    task_id = "GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST"
    log(task_id, "Starting GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST_inner():
            page = "https://matsne.gov.ge/ka/document/view/4234552?publication=0"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//table[12]").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def GG_GFSC_DD():
    task_id = "GG_GFSC_DD"
    log(task_id, "Starting GG_GFSC_DD …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def GG_GFSC_DD_inner():
            page = "https://www.gfsc.gg/commission/enforcement/disqualified-directors"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, '//*[@id="block-gfsc-theme-partial-rows"]/div/section[2]/div').text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = GG_GFSC_DD_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "GG_GFSC_DD", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "GG_GFSC_DD", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def HK_ICAC_WL():
    task_id = "HK_ICAC_WL"
    log(task_id, "Starting HK_ICAC_WL …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def HK_ICAC_WL_inner():
            website = 'https://www.icac.org.hk/en/rc/wanted/index.html'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="mainContent"]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = HK_ICAC_WL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "HK_ICAC_WL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "HK_ICAC_WL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def HU_MNB_MSW():
    task_id = "HU_MNB_MSW"
    log(task_id, "Starting HU_MNB_MSW …")
    try:
        import hashlib
        import openpyxl
        import json
        import time
        import os
        import glob
        import tempfile
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def HU_MNB_MSW_inner():
            download_dir = tempfile.mkdtemp()
            print(f"Download dir : {download_dir}")
            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_experimental_option("prefs", {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            })
            driver = webdriver.Chrome(options=options)
            driver.get(
                "https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations"
                "/compliance-actions-and-activities/warning-letters"
            )
            wait = WebDriverWait(driver, 30)
            print("Waiting for Export Excel button...")
            export_btn = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[class*='excel-export']")
                )
            )
            print(f"Found button: '{export_btn.text}'")
            time.sleep(3)
            driver.execute_script("arguments[0].click();", export_btn)
            print("Clicked Export Excel — waiting for download...")
            xlsx_file = None
            for i in range(120):
                files = glob.glob(os.path.join(download_dir, "*.xlsx"))
                complete = [f for f in files if not f.endswith(".crdownload")]
                if complete:
                    xlsx_file = complete[0]
                    print(f"Download complete: {xlsx_file}")
                    break
                time.sleep(1)
                if (i + 1) % 10 == 0:
                    print(f"  Still waiting... {i+1}s")
            driver.quit()
            if not xlsx_file:
                print("Download timed out.")
                return None
            print("\nParsing XLSX...")
            wb = openpyxl.load_workbook(xlsx_file, read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            wb.close()
            col_headers = [str(h) if h is not None else "" for h in rows[0]]
            data_rows = rows[1:]
            print(f"Columns      : {col_headers}")
            print(f"Total rows   : {len(data_rows)}")
            print(f"Sample:\n{json.dumps(dict(zip(col_headers, [str(v) for v in data_rows[0]])), indent=2)}")
            with open(xlsx_file, "rb") as f:
                file_bytes = f.read()
            output_hash = hashlib.sha256(file_bytes).hexdigest()
            print(output_hash)
            os.remove(xlsx_file)
            os.rmdir(download_dir)
            return output_hash, data_rows
        _result = HU_MNB_MSW_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "HU_MNB_MSW", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "HU_MNB_MSW", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IE_CBI_PN():
    task_id = "IE_CBI_PN"
    log(task_id, "Starting IE_CBI_PN …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def IE_CBI_PN_inner():
            page = "https://www.centralbank.ie/news-media/legal-notices/prohibition-notices"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//*[contains(text(),'Current Prohibition Notices')]//following::h3").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = IE_CBI_PN_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IE_CBI_PN", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IE_CBI_PN", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IL_IIPB_PIL():
    task_id = "IL_IIPB_PIL"
    log(task_id, "Starting IL_IIPB_PIL …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def IL_IIPB_PIL_inner():
            website = 'https://iipb.illinois.gov/prohibited-investment-list.html'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//h1[contains(text(),'Prohibited Investment List')]/ancestor::div[4]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = IL_IIPB_PIL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IL_IIPB_PIL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IL_IIPB_PIL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IL_MOF_WMD():
    task_id = "IL_MOF_WMD"
    log(task_id, "Starting IL_MOF_WMD …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def IL_MOF_WMD_inner():
            page = "https://www.gov.il/he/pages/declared_elements_list"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link2 = driver.find_element(By.XPATH, '//*[@id="filesToDownload_item_0_item_1"]').get_attribute("href")
            print(Link2)
            driver.close()
            response = requests.get(Link2)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = IL_MOF_WMD_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IL_MOF_WMD", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IL_MOF_WMD", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IL_MOJ_TERR_ORG():
    task_id = "IL_MOJ_TERR_ORG"
    log(task_id, "Starting IL_MOJ_TERR_ORG …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def IL_MOJ_TERR_ORG_inner():
            website = 'https://nbctf.mod.gov.il/he/MinisterSanctions/Announcements/Pages/nbctfDownloads.aspx'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ctl00_PlaceHolderMain_ctl02_ctl02__ControlWrapper_RichHtmlField"]/table[1]/tbody').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = IL_MOJ_TERR_ORG_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IL_MOJ_TERR_ORG", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IL_MOJ_TERR_ORG", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IM_FSA_DD():
    task_id = "IM_FSA_DD"
    log(task_id, "Starting IM_FSA_DD …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def IM_FSA_DD_inner():
            page = "https://www.iomfsa.im/enforcement/disqualified-directors/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "/html/body/div[3]/div/div").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = IM_FSA_DD_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IM_FSA_DD", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IM_FSA_DD", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IM_FSA_ENFORCEMENT():
    task_id = "IM_FSA_ENFORCEMENT"
    log(task_id, "Starting IM_FSA_ENFORCEMENT …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def IM_FSA_ENFORCEMENT_inner():
            page = "https://www.iomfsa.im/enforcement/enforcement-action/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//h1[contains(text(),'Enforcement Action')]//following::div").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = IM_FSA_ENFORCEMENT_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IM_FSA_ENFORCEMENT", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IM_FSA_ENFORCEMENT", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def INTERPOL_UN_Notices_Entities():
    task_id = "INTERPOL_UN_Notices_Entities"
    log(task_id, "Starting INTERPOL_UN_Notices_Entities …")
    try:
        import time
        import json
        import hashlib
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        def INTERPOL_UN_Notices_Entities_inner():
            api_url = (
                "https://ws-public.interpol.int/notices/v1/un/entities"
                "?resultPerPage=160&page=1"
            )
            options = Options()
            # options.add_argument("--headless")   # uncomment for headless mode
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=options)
            try:
                print("Fetching INTERPOL UN Notices (Entities)...")
                driver.get("https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Entities")
                time.sleep(3)
                driver.get(api_url)
                time.sleep(2)
                raw = driver.find_element("tag name", "body").text
                data = json.loads(raw)
                total   = data.get("total", 0)
                notices = data.get("_embedded", {}).get("notices", [])
                print(f"API reports total: {total} | Retrieved: {len(notices)}")
                all_entries = []
                for notice in notices:
                    entity_id     = notice.get("entity_id", "N/A")
                    name          = notice.get("name", "N/A")
                    un_reference  = notice.get("un_reference", "N/A")
                    sanctions     = notice.get("sanctions_references", [])
                    sanctions_str = ", ".join(sanctions) if sanctions else "N/A"
                    self_href     = notice.get("_links", {}).get("self", {}).get("href", "N/A")

                    entry = (
                        f"ID: {entity_id} | Name: {name} | "
                        f"UN Ref: {un_reference} | Sanctions: {sanctions_str} | "
                        f"URL: {self_href}"
                    )
                    all_entries.append(entry)
                Data = "\n".join(all_entries)
                print(Data)
                print(f"\nTotal entries extracted: {len(all_entries)}")
                output_hash = hashlib.sha256(Data.encode("utf-8")).hexdigest()
                print(output_hash)
                return Data, output_hash
            finally:
                driver.quit()
        if False:
            INTERPOL_UN_Notices_Entities_inner()
        _result = INTERPOL_UN_Notices_Entities_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "INTERPOL_UN_Notices_Entities", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "INTERPOL_UN_Notices_Entities", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def INTERPOL_UN_Notices_Individuals():
    task_id = "INTERPOL_UN_Notices_Individuals"
    log(task_id, "Starting INTERPOL_UN_Notices_Individuals …")
    try:
        import time
        import json
        import hashlib
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        def INTERPOL_UN_Notices_Individuals_inner():
            seed_url = "https://www.interpol.int/en/How-we-work/Notices/View-UN-Notices-Individuals"
            api_url  = "https://ws-public.interpol.int/notices/v1/un/persons"
            options = Options()
            # options.add_argument("--headless")   # uncomment for headless mode
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=options)
            try:
                print("Fetching INTERPOL UN Notices (Individuals)...")
                driver.get(seed_url)
                time.sleep(3)
                all_entries = []
                page = 1
                total = None
                while True:
                    paged_url = f"{api_url}?resultPerPage=160&page={page}"
                    print(f"  Fetching page {page}: {paged_url}")
                    driver.get(paged_url)
                    time.sleep(2)
                    raw = driver.find_element("tag name", "body").text
                    data = json.loads(raw)
                    if total is None:
                        total = data.get("total", 0)
                        pages_needed = -(-total // 160)  # ceiling division
                        print(f"API reports total: {total} records across {pages_needed} pages")
                    notices = data.get("_embedded", {}).get("notices", [])
                    if not notices:
                        print(f"  No notices returned on page {page}, stopping.")
                        break
                    for notice in notices:
                        entity_id     = notice.get("entity_id", "N/A")
                        forename      = notice.get("forename", "N/A")
                        name          = notice.get("name", "N/A")
                        dob           = notice.get("date_of_birth", "N/A")
                        nats          = notice.get("nationalities", [])
                        nat_str       = ", ".join(nats) if nats else "N/A"
                        un_reference  = notice.get("un_reference", "N/A")
                        sanctions     = notice.get("sanctions_references", [])
                        sanctions_str = ", ".join(sanctions) if sanctions else "N/A"
                        self_href     = notice.get("_links", {}).get("self", {}).get("href", "N/A")
                        entry = (
                            f"ID: {entity_id} | Name: {forename} {name} | "
                            f"DOB: {dob} | Nationalities: {nat_str} | "
                            f"UN Ref: {un_reference} | Sanctions: {sanctions_str} | "
                            f"URL: {self_href}"
                        )
                        all_entries.append(entry)
                    print(f"  Page {page} done: fetched {len(notices)} (cumulative: {len(all_entries)}/{total})")
                    if len(all_entries) >= total:
                        print("  All records collected.")
                        break
                    page += 1
                    time.sleep(1)
                Data = "\n".join(all_entries)
                print(Data)
                print(f"\nTotal entries extracted: {len(all_entries)}")
                output_hash = hashlib.sha256(Data.encode("utf-8")).hexdigest()
                print(output_hash)
                return Data, output_hash
            finally:
                driver.quit()
        if False:
            INTERPOL_UN_Notices_Individuals_inner()
        _result = INTERPOL_UN_Notices_Individuals_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "INTERPOL_UN_Notices_Individuals", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "INTERPOL_UN_Notices_Individuals", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IN_BANNED_ORG():
    task_id = "IN_BANNED_ORG"
    log(task_id, "Starting IN_BANNED_ORG …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def IN_BANNED_ORG_inner():
            website = 'https://www.mha.gov.in/en/divisionofmha/counter-terrorism-and-counter-radicalization-division/Banned-Organizations'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            time.sleep(5)
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//th[contains(text(),'Download/Link')]/following::a").get_attribute('href')
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = IN_BANNED_ORG_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IN_BANNED_ORG", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IN_BANNED_ORG", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IN_SEBI_DEBARRED_ENTITIES():
    task_id = "IN_SEBI_DEBARRED_ENTITIES"
    log(task_id, "Starting IN_SEBI_DEBARRED_ENTITIES …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def IN_SEBI_DEBARRED_ENTITIES_inner():
            page = "https://www.bseindia.com/investors/debent"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//span[contains(text(),'Debarred Entities')]//following::div[4]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = IN_SEBI_DEBARRED_ENTITIES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IN_SEBI_DEBARRED_ENTITIES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IN_SEBI_DEBARRED_ENTITIES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def IN_UNLAWFUL_ASSOCIATION():
    task_id = "IN_UNLAWFUL_ASSOCIATION"
    log(task_id, "Starting IN_UNLAWFUL_ASSOCIATION …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def IN_UNLAWFUL_ASSOCIATION_inner():
            page = "https://www.mha.gov.in/en/commoncontent/unlawful-associations-under-section-3-of-unlawful-activities-prevention-act-1967"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//table").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = IN_UNLAWFUL_ASSOCIATION_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "IN_UNLAWFUL_ASSOCIATION", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "IN_UNLAWFUL_ASSOCIATION", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_METI():
    task_id = "JP_METI"
    log(task_id, "Starting JP_METI …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def JP_METI_inner():
            page = "https://www.meti.go.jp/policy/anpo/englishpage.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Review of the End User List')]").text
            print(Link)
            driver.close()
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = JP_METI_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_METI", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_METI", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_CAR_2134():
    task_id = "JP_UN_CAR_2134"
    log(task_id, "Starting JP_UN_CAR_2134 …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_CAR_2134_inner():
            website = 'https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/central-afrikan.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_CAR_2134_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_CAR_2134", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_CAR_2134", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_CONGO():
    task_id = "JP_UN_CONGO"
    log(task_id, "Starting JP_UN_CONGO …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def JP_UN_CONGO_inner():
            page = "https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/CongoRD.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(10)
            #iframe = driver.find_element(By.XPATH, '//*[@id="myFrame"]')
            #driver.switch_to.frame(iframe)
            Data = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]/table').text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_CONGO_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_CONGO", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_CONGO", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_SANCTIONS():
    task_id = "JP_UN_SANCTIONS"
    log(task_id, "Starting JP_UN_SANCTIONS …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_SANCTIONS_inner():
            website = 'https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/libya.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_SANCTIONS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_SANCTIONS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_SANCTIONS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_SANCTIONS_IRAN():
    task_id = "JP_UN_SANCTIONS_IRAN"
    log(task_id, "Starting JP_UN_SANCTIONS_IRAN …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_SANCTIONS_IRAN_inner():
            website = 'https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/iran.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_SANCTIONS_IRAN_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_SANCTIONS_IRAN", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_SANCTIONS_IRAN", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_SANCTIONS_SOMALIA():
    task_id = "JP_UN_SANCTIONS_SOMALIA"
    log(task_id, "Starting JP_UN_SANCTIONS_SOMALIA …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_SANCTIONS_SOMALIA_inner():
            website = 'https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/somalia.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_SANCTIONS_SOMALIA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_SANCTIONS_SOMALIA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_SANCTIONS_SOMALIA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_SANCTIONS_SYRIA():
    task_id = "JP_UN_SANCTIONS_SYRIA"
    log(task_id, "Starting JP_UN_SANCTIONS_SYRIA …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_SANCTIONS_SYRIA_inner():
            website = 'https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/syria.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_SANCTIONS_SYRIA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_SANCTIONS_SYRIA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_SANCTIONS_SYRIA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_SS_2206():
    task_id = "JP_UN_SS_2206"
    log(task_id, "Starting JP_UN_SS_2206 …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_SS_2206_inner():
            website = 'http://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/south-sudan.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_SS_2206_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_SS_2206", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_SS_2206", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def JP_UN_YE_2140():
    task_id = "JP_UN_YE_2140"
    log(task_id, "Starting JP_UN_YE_2140 …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def JP_UN_YE_2140_inner():
            website = 'https://www.meti.go.jp/policy/external_economy/trade_control/01_seido/04_seisai/yemen.html'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="ColA2011"]/div[3]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = JP_UN_YE_2140_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "JP_UN_YE_2140", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "JP_UN_YE_2140", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def KE_FRC_DTFS():
    task_id = "KE_FRC_DTFS"
    log(task_id, "Starting KE_FRC_DTFS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def KE_FRC_DTFS_inner():
            page = "https://www.frc.go.ke/?page_id=193"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Domestic List Kenya')]").get_attribute("href")
            print(Link)
            driver.close()
            response = requests.get(Link)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = KE_FRC_DTFS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "KE_FRC_DTFS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "KE_FRC_DTFS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def KG_FIU_SANCTIONS_ENTITIES():
    task_id = "KG_FIU_SANCTIONS_ENTITIES"
    log(task_id, "Starting KG_FIU_SANCTIONS_ENTITIES …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from pypdf import PdfReader
        from bs4 import BeautifulSoup
        from io import BytesIO
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.devtools.v143.dom import get_attributes
        def KG_FIU_SANCTIONS_ENTITIES_inner():
            page = "https://fiu.gov.kg/sked/9"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'PDF')]").get_attribute("href")
            print(Link)
            driver.close()
            response = requests.get(Link)
            soup = BeautifulSoup(response.content, "html.parser")
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
                print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = KG_FIU_SANCTIONS_ENTITIES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "KG_FIU_SANCTIONS_ENTITIES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "KG_FIU_SANCTIONS_ENTITIES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def KG_FIU_SANCTIONS_INDIVIDUALS():
    task_id = "KG_FIU_SANCTIONS_INDIVIDUALS"
    log(task_id, "Starting KG_FIU_SANCTIONS_INDIVIDUALS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from pypdf import PdfReader
        from bs4 import BeautifulSoup
        from io import BytesIO
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.devtools.v143.dom import get_attributes
        def KG_FIU_SANCTIONS_INDIVIDUALS_inner():
            page = "https://fiu.gov.kg/sked/9"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'PDF')]").get_attribute("href")
            print(Link)
            driver.close()
            response = requests.get(Link)
            soup = BeautifulSoup(response.content, "html.parser")
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
                print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = KG_FIU_SANCTIONS_INDIVIDUALS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "KG_FIU_SANCTIONS_INDIVIDUALS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "KG_FIU_SANCTIONS_INDIVIDUALS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def KR_MFA_SAKP():
    task_id = "KR_MFA_SAKP"
    log(task_id, "Starting KR_MFA_SAKP …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def KR_MFA_SAKP_inner():
            website = 'https://www.mofa.go.kr/www/wpge/m_25834/contents.do'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="contents"]/ul/li/ul/li[2]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = KR_MFA_SAKP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "KR_MFA_SAKP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "KR_MFA_SAKP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def LB_ISF_NTFL():
    task_id = "LB_ISF_NTFL"
    log(task_id, "Starting LB_ISF_NTFL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def LB_ISF_NTFL_inner():
            page = "https://isf.gov.lb/national-terrorism-financial-list/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link2 = driver.find_element(By.XPATH, "//a[contains(text(),'Click')]").text
            print(Link2)
            driver.close()
            output_date = hashlib.sha256(Link2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = LB_ISF_NTFL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "LB_ISF_NTFL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "LB_ISF_NTFL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def LV_FIU_SANCTIONED_SUBJECTS():
    task_id = "LV_FIU_SANCTIONED_SUBJECTS"
    log(task_id, "Starting LV_FIU_SANCTIONED_SUBJECTS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def LV_FIU_SANCTIONED_SUBJECTS_inner():
            page = "https://sankcijas.fid.gov.lv/sankciju-subjekti"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Informācija')]//following::a[contains(text(),'Sankciju subjekti')]").get_attribute("href")
            print(Link)
            driver.close()
            response = requests.get(Link)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = LV_FIU_SANCTIONED_SUBJECTS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "LV_FIU_SANCTIONED_SUBJECTS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "LV_FIU_SANCTIONED_SUBJECTS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MC_BT_NAFL():
    task_id = "MC_BT_NAFL"
    log(task_id, "Starting MC_BT_NAFL …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def MC_BT_NAFL_inner():
            page = "https://geldefonds.gouv.mc/en/national-asset-freezing-list?query=&displaylatestdm=false&sort_by=update_desc&limit=16"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//p[contains(text(),'Last updated')]").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = MC_BT_NAFL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MC_BT_NAFL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MC_BT_NAFL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MN_OSP_SDVR():
    task_id = "MN_OSP_SDVR"
    log(task_id, "Starting MN_OSP_SDVR …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def MN_OSP_SDVR_inner():
            website = 'https://mn.gov/admin/osp/government/suspended-debarred/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="content"]/div/div/div[1]/div[2]').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MN_OSP_SDVR_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MN_OSP_SDVR", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MN_OSP_SDVR", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MT_MPF_WANTED():
    task_id = "MT_MPF_WANTED"
    log(task_id, "Starting MT_MPF_WANTED …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def MT_MPF_WANTED_inner():
            website = 'https://pulizija.gov.mt/en/appeals/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "/html/body/div[1]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MT_MPF_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MT_MPF_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MT_MPF_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MY_BNM_WANTED():
    task_id = "MY_BNM_WANTED"
    log(task_id, "Starting MY_BNM_WANTED …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def MY_BNM_WANTED_inner():
            website = 'https://www.bnm.gov.my/have-you-seen'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//p[contains(text(),'Have You Seen These Individuals?')]//following::div").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MY_BNM_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MY_BNM_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MY_BNM_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MY_BNM_WARNING_LETTERS():
    task_id = "MY_BNM_WARNING_LETTERS"
    log(task_id, "Starting MY_BNM_WARNING_LETTERS …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def MY_BNM_WARNING_LETTERS_inner():
            page = "https://www.bnm.gov.my/enforcement-actions/warning-letters"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//*[contains(text(),'Date Warning Letter Sent')]//following::td[4]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MY_BNM_WARNING_LETTERS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MY_BNM_WARNING_LETTERS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MY_BNM_WARNING_LETTERS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MY_MHA_MOHA_LIST():
    task_id = "MY_MHA_MOHA_LIST"
    log(task_id, "Starting MY_MHA_MOHA_LIST …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def MY_MHA_MOHA_LIST_inner():
            website = 'https://www.moha.gov.my/utama/index.php/en/component/content/article/350-list-of-ministries-of-home-affairs'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Last updated on')]//following::div").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MY_MHA_MOHA_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MY_MHA_MOHA_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MY_MHA_MOHA_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MY_SCM_IA():
    task_id = "MY_SCM_IA"
    log(task_id, "Starting MY_SCM_IA …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def MY_SCM_IA_inner():
            page = "https://www.sc.com.my/investor-alert-list"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(10)
            #iframe = driver.find_element(By.XPATH, '//*[@id="myFrame"]')
            #driver.switch_to.frame(iframe)
            Data = driver.find_element(By.XPATH, "//b[contains(text(),'Name')]//ancestor::div").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MY_SCM_IA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MY_SCM_IA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MY_SCM_IA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def MY_SCM_WANTED():
    task_id = "MY_SCM_WANTED"
    log(task_id, "Starting MY_SCM_WANTED …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def MY_SCM_WANTED_inner():
            website = 'https://www.sc.com.my/regulation/enforcement/have-you-seen-these-persons/persons-subject-to-arrest'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//div[contains(text(),'Persons Subject to Arrest')]//following::div[7]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = MY_SCM_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "MY_SCM_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "MY_SCM_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def NG_EFCC_WANTED_PERSON():
    task_id = "NG_EFCC_WANTED_PERSON"
    log(task_id, "Starting NG_EFCC_WANTED_PERSON …")
    try:
        import time, io, hashlib, re, math
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def NG_EFCC_WANTED_PERSON_inner():
            base_url = "https://www.efcc.gov.ng/WantedPersons?page={}"
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--window-size=1920,1080")
            driver = webdriver.Chrome(options=options)
            wait = WebDriverWait(driver, 30)
            driver.get(base_url.format(1))
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(5)
            body_text = driver.find_element(By.TAG_NAME, "body").text
            max_page = 1
            match = re.search(r"Page\s+\d+\s+of\s+(\d+)", body_text, re.I)
            if match:
                max_page = int(match.group(1))
            page_links = driver.find_elements(By.XPATH, "//*[normalize-space(text()) and string-length(normalize-space(text())) <= 3]")
            for link in page_links:
                txt = link.text.strip()
                if txt.isdigit():
                    max_page = max(max_page, int(txt))
            # Method 3: extract total wanted persons, example: "155 Wanted Persons"
            total_match = re.search(r"(\d+)\s+Wanted\s+Persons", body_text, re.I)
            if total_match and max_page == 1:
                total_records = int(total_match.group(1))
                max_page = math.ceil(total_records / 8)  # EFCC page usually shows 8 records per page
            print("Total pages detected:", max_page)
            names = []
            for page_no in range(1, max_page + 1):
                print(f"Scraping page {page_no}...")
                driver.get(base_url.format(page_no))
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(5)
                cards = driver.find_elements(
                    By.XPATH,
                    "//*[contains(text(),'/')]/ancestor::*[string-length(normalize-space(.)) > 10][1]"
                )
                for card in cards:
                    lines = [x.strip() for x in card.text.split("\n") if x.strip()]
                    for i, line in enumerate(lines):
                        if re.search(r"\d{1,2}/\d{1,2}/\d{4}", line) and i > 0:
                            name = lines[i - 1].strip()
                            if (
                                name
                                and name.upper() == name
                                and not any(x in name.lower() for x in [
                                    "wanted persons",
                                    "page",
                                    "home",
                                    "read more",
                                    "efcc"
                                ])
                            ):
                                names.append(name)
            driver.quit()
            names = list(dict.fromkeys(names))
            print("\nExtracted Names:\n")
            for i, name in enumerate(names, 1):
                print(f"{i}. {name}")
            Data = "|".join(names)
            output_hash = hashlib.sha256(Data.encode("utf-8")).hexdigest()
            print(output_hash)
            return output_hash
        _result = NG_EFCC_WANTED_PERSON_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "NG_EFCC_WANTED_PERSON", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "NG_EFCC_WANTED_PERSON", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def NG_NIGSAC_ENTITIES():
    task_id = "NG_NIGSAC_ENTITIES"
    log(task_id, "Starting NG_NIGSAC_ENTITIES …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def NG_NIGSAC_ENTITIES_inner():
            page = "https://nigsac.gov.ng/IndSancList"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            driver.execute_script("""
                var tables = document.querySelectorAll('table');
                tables.forEach(function(t) {
                    if ($.fn && $.fn.dataTable && $.fn.dataTable.isDataTable(t)) {
                        $(t).DataTable().page.len(-1).draw();
                    }
                });
            """)
            time.sleep(2)
            try:
                length_selects = driver.find_elements(By.CSS_SELECTOR, "select[name*='DataTables_Table']")
                for sel in length_selects:
                    for opt in sel.find_elements(By.TAG_NAME, "option"):
                        if opt.get_attribute("value") == "-1" or opt.text.strip().lower() == "all":
                            opt.click()
                            break
                time.sleep(2)
            except Exception:
                pass
            raw = driver.execute_script("""
                var table = document.querySelectorAll('table')[1];
                var rows = table.querySelectorAll('tbody tr');
                var result = [];
                rows.forEach(function(row) {
                    var cells = row.querySelectorAll('td');
                    if (cells.length >= 4) {
                        result.push({
                            'S/N':         cells[0].innerText.trim(),
                            'Entity Name': cells[1].innerText.trim(),
                            'Comments':    cells[2].innerText.trim(),
                            'Record Date': cells[3].innerText.trim()
                        });
                    }
                });
                return result;
            """)
            driver.close()
            df = pd.DataFrame(raw)
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_csv(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = NG_NIGSAC_ENTITIES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "NG_NIGSAC_ENTITIES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "NG_NIGSAC_ENTITIES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def NG_NIGSAC_INDIVIDUALS():
    task_id = "NG_NIGSAC_INDIVIDUALS"
    log(task_id, "Starting NG_NIGSAC_INDIVIDUALS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def NG_NIGSAC_INDIVIDUALS_inner():
            page = "https://nigsac.gov.ng/IndSancList"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            driver.execute_script("""
                var tables = document.querySelectorAll('table');
                tables.forEach(function(t) {
                    if ($.fn && $.fn.dataTable && $.fn.dataTable.isDataTable(t)) {
                        $(t).DataTable().page.len(-1).draw();
                    }
                });
            """)
            time.sleep(2)
            try:
                length_selects = driver.find_elements(By.CSS_SELECTOR, "select[name*='DataTables_Table']")
                for sel in length_selects:
                    for opt in sel.find_elements(By.TAG_NAME, "option"):
                        if opt.get_attribute("value") == "-1" or opt.text.strip().lower() == "all":
                            opt.click()
                            break
                time.sleep(2)
            except Exception:
                pass
            tables = driver.find_elements(By.TAG_NAME, "table")
            rows = tables[0].find_elements(By.TAG_NAME, "tr")
            data = []
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 6:
                    data.append({
                        "S/N":           cols[0].text.strip(),
                        "First Name":    cols[1].text.strip(),
                        "Surname":       cols[2].text.strip(),
                        "Nationality":   cols[3].text.strip(),
                        "Birth Country": cols[4].text.strip(),
                        "Record Date":   cols[5].text.strip(),
                    })
            driver.close()
            df = pd.DataFrame(data)
            print(df.to_csv(index=False))
            output_date = hashlib.sha256(df.to_csv(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = NG_NIGSAC_INDIVIDUALS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "NG_NIGSAC_INDIVIDUALS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "NG_NIGSAC_INDIVIDUALS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def NO_BLACK_LIST():
    task_id = "NO_BLACK_LIST"
    log(task_id, "Starting NO_BLACK_LIST …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def NO_BLACK_LIST_inner():
            website = 'https://www.fiskeridir.no/english/fisheries/iuu-and-the-norwegian-blacklist'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//a[contains(text(),'Norwegian Black List (last updated')]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode('utf-8')).hexdigest()
            print(output_date)
        _result = NO_BLACK_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "NO_BLACK_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "NO_BLACK_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def NY_OGS_IDA():
    task_id = "NY_OGS_IDA"
    log(task_id, "Starting NY_OGS_IDA …")
    try:
        import hashlib
        import requests
        import time
        import io
        import pdfplumber
        from pypdf import PdfReader
        from io import BytesIO
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def NY_OGS_IDA_inner():
            website = 'https://ogs.ny.gov/iran-divestment-act-2012'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            PDF = driver.find_element(By.XPATH, "/html/body/div[3]/div/main/div/div/div[2]/div[2]/div/div/div[3]/article/div/ul/li/div/h3/a").get_attribute('href')
            print(PDF)
            driver.quit()
            response = requests.get(PDF)
            response.raise_for_status()
            print(response.content)
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            output_date = hashlib.sha256(pdf_bytes).hexdigest()
            print(output_date)
        _result = NY_OGS_IDA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "NY_OGS_IDA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "NY_OGS_IDA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def OIG_MOST_WANTED():
    task_id = "OIG_MOST_WANTED"
    log(task_id, "Starting OIG_MOST_WANTED …")
    try:
        import requests
        import hashlib
        from bs4 import BeautifulSoup
        def OIG_MOST_WANTED_inner():
            BASE_URL = "https://oig.hhs.gov"
            START_URL = f"{BASE_URL}/fraud/fugitives/"
            HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

            session = requests.Session()
            session.headers.update(HEADERS)

            names = []
            page = 1

            while True:
                url = START_URL if page == 1 else f"{START_URL}?page={page}"
                resp = session.get(url, timeout=20)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")

                links = soup.find_all("a", href=lambda h: h and h.startswith("/fraud/fugitives/")
                                                          and h != "/fraud/fugitives/")
                if not links:
                    break

                for a in links:
                    name = a.get_text(strip=True)
                    if name:
                        names.append(name)

                next_link = soup.find("a", string=lambda t: t and "Next" in t)
                if not next_link:
                    break
                page += 1

            print(f"Total fugitives: {len(names)}\n")
            for i, name in enumerate(names, 1):
                print(f"{i:>3}. {name}")

            content = "\n".join(names)
            output_date = hashlib.sha256(content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = OIG_MOST_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "OIG_MOST_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "OIG_MOST_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def OM_NCTC_LL():
    task_id = "OM_NCTC_LL"
    log(task_id, "Starting OM_NCTC_LL …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def OM_NCTC_LL_inner():
            page = "https://www.nctc.gov.om/TargetedFinancialSanctions/LocalList"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '/html/body/div[1]/main/div[2]').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = OM_NCTC_LL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "OM_NCTC_LL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "OM_NCTC_LL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PA_TREAS_SCI():
    task_id = "PA_TREAS_SCI"
    log(task_id, "Starting PA_TREAS_SCI …")
    try:
        import hashlib
        import requests
        import time
        import re
        import pdfplumber
        from pypdf import PdfReader
        from io import BytesIO
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PA_TREAS_SCI_inner():
            website = 'https://www.patreasury.gov/divestment/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            PDF = driver.find_element(By.XPATH, "//*[contains(text(),'Act 44 Report ')]").get_attribute("onclick")
            print(PDF)
            PDF_PATH = PDF.split("window.open(")[1].strip("')\"")
            PDF_PATH_1 = PDF_PATH.lstrip(".")
            print(PDF_PATH_1)
            driver.quit()
            url1="https://www.patreasury.gov/"+PDF_PATH_1
            response = requests.get(url1)
            response.raise_for_status()
            print(response.content)
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            output_date = hashlib.sha256(pdf_bytes).hexdigest()
            print(output_date)
        _result = PA_TREAS_SCI_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PA_TREAS_SCI", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PA_TREAS_SCI", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PE_MEF_DISQUALIFIED_SUPPLIERS():
    task_id = "PE_MEF_DISQUALIFIED_SUPPLIERS"
    log(task_id, "Starting PE_MEF_DISQUALIFIED_SUPPLIERS …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PE_MEF_DISQUALIFIED_SUPPLIERS_inner():
            page = "https://www.gob.pe/institucion/oece/informes-publicaciones/297360-relacion-de-proveedores-con-inhabilitacion-vigente-por-mandato-judicial"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'LISTADO_INHABILITADOS')]//following::a[1]").get_attribute("href")
            print(Link)
            response = requests.get(Link)
            soup = BeautifulSoup(response.content, "html.parser")
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
                print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PE_MEF_DISQUALIFIED_SUPPLIERS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PE_MEF_DISQUALIFIED_SUPPLIERS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PE_MEF_DISQUALIFIED_SUPPLIERS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PL_MOF_SRM():
    task_id = "PL_MOF_SRM"
    log(task_id, "Starting PL_MOF_SRM …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PL_MOF_SRM_inner():
            page = "https://www.gov.pl/web/finanse/lista-osob-i-podmiotow-wobec-ktorych-stosuje-sie-szczegolne-srodki-ograniczajace-na-podstawie-art-118-ustawy-z-dnia-1-marca-2018-r-o-przeciwdzialaniu-praniu-pieniedzy-i-finansowaniu-terroryzmu"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link2 = driver.find_element(By.XPATH, "//*[contains(text(),'wersja xlsx')]//ancestor::a").get_attribute("href")
            print(Link2)
            driver.close()
            response = requests.get(Link2)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PL_MOF_SRM_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PL_MOF_SRM", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PL_MOF_SRM", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_AK_DHSS_EPL():
    task_id = "PRM_AK_DHSS_EPL"
    log(task_id, "Starting PRM_AK_DHSS_EPL …")
    try:
        import hashlib
        import requests
        import time
        import re
        import pdfplumber
        from pypdf import PdfReader
        from io import BytesIO
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_AK_DHSS_EPL_inner():
            url1="https://health.alaska.gov/media/h3ueo2kf/alaska-medical-assistance-excluded-provider-list.pdf"
            response = requests.get(url1)
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_AK_DHSS_EPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_AK_DHSS_EPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_AK_DHSS_EPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_AL_AMA_SP():
    task_id = "PRM_AL_AMA_SP"
    log(task_id, "Starting PRM_AL_AMA_SP …")
    try:
        import hashlib
        import requests
        from bs4 import BeautifulSoup
        def PRM_AL_AMA_SP_inner():
            url1="https://medicaid.alabama.gov/content/8.0_Fraud/8.7_Suspended_Providers.aspx"
            response = requests.get(url1)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            Page = soup.find("div", {"class": "col-lg-8"})
            Page2 = Page.find("ul").text
            output_date = hashlib.sha256(Page2.encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_AL_AMA_SP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_AL_AMA_SP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_AL_AMA_SP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_AR_DHS_EP():
    task_id = "PRM_AR_DHS_EP"
    log(task_id, "Starting PRM_AR_DHS_EP …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        PAGE_URL = "https://dhs.arkansas.gov/dhs/portal/Exclusions/PublicSearch/"
        LINK_XPATH = '//*[@id="s4-bodyContainer"]/div[6]/form/div[6]/div/a'
        def PRM_AR_DHS_EP_inner():
            options = Options()
            # options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            try:
                driver.get(PAGE_URL)
                link_element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, LINK_XPATH))
                )
                download_url = link_element.get_attribute("href")
                print(f"Download URL: {download_url}")
            finally:
                driver.quit()
            response = requests.get(download_url, timeout=30)
            response.raise_for_status()
            raw_text = response.content.decode("utf-8", errors="replace")
            lines = raw_text.splitlines()
            print("=== RAW PREVIEW ===")
            for i, line in enumerate(lines[:3]):
                print(f"Line {i}: {line}")
            print(f"Total lines: {len(lines)}")
            print("===================")
            df = pd.read_csv(
                io.StringIO(raw_text),
                skiprows=1,
                on_bad_lines='warn',   # 'skip' to silently drop, 'warn' to log them
                engine='python'
            )
            print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            print(f"Columns: {df.columns.tolist()}")
            print(df.head())
            csv_data = df.to_csv(index=False)
            output_date = hashlib.sha256(csv_data.encode("utf-8")).hexdigest()
            print(output_date)
            return df, output_date
        if False:
            dataframe, output_date = PRM_AR_DHS_EP_inner()
        _result = PRM_AR_DHS_EP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_AR_DHS_EP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_AR_DHS_EP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_CA_DHCS_SIPL():
    task_id = "PRM_CA_DHCS_SIPL"
    log(task_id, "Starting PRM_CA_DHCS_SIPL …")
    try:
        import hashlib
        import requests
        import pandas as pd
        import io
        from bs4 import BeautifulSoup
        def PRM_CA_DHCS_SIPL_inner():
            website = 'https://data.chhs.ca.gov/dataset/provider-suspended-and-ineligible-list-s-i-list'
            r = requests.get(website).text
            soup = BeautifulSoup(r, "html.parser")
            Page1 = soup.find('ul', class_="resource-list")
            Page2 = Page1.find('div', class_="actions")
            Page3 = Page2.find('a')
            HREF = Page3.attrs['href']
            r1 = requests.get(HREF)
            r1.raise_for_status()
            csv_bytes = r1.content
            df = pd.read_csv(io.BytesIO(csv_bytes), encoding="latin-1")
            Data = df.to_csv(index=False)
            output_date = hashlib.sha256(str(Data).encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_CA_DHCS_SIPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_CA_DHCS_SIPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_CA_DHCS_SIPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_CA_DIR_DC():
    task_id = "PRM_CA_DIR_DC"
    log(task_id, "Starting PRM_CA_DIR_DC …")
    try:
        import hashlib
        import requests
        from bs4 import BeautifulSoup
        def PRM_CA_DIR_DC_inner():
            website = 'https://www.dir.ca.gov/dlse/debar.html'
            r = requests.get(website).text
            soup = BeautifulSoup(r, "html.parser")
            Content = soup.find("table", class_="table")
            output_date = hashlib.sha256(str(Content).encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_CA_DIR_DC_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_CA_DIR_DC", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_CA_DIR_DC", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_DC_DDS_PSL():
    task_id = "PRM_DC_DDS_PSL"
    log(task_id, "Starting PRM_DC_DDS_PSL …")
    try:
        import hashlib
        import requests
        from io import BytesIO
        from pypdf import PdfReader
        from playwright.sync_api import sync_playwright
        def PRM_DC_DDS_PSL_inner():
            website = 'https://dds.dc.gov/publication/provider-sanctions-list'
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(website)
                page.wait_for_load_state("networkidle")
                link = page.locator("text=Provider Sanctions List-UPDATED").first
                content_url = link.get_attribute("href")
                print(content_url)
                browser.close()
            response = requests.get(content_url)
            response.raise_for_status()
            pdf_file = BytesIO(response.content)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_DC_DDS_PSL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_DC_DDS_PSL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_DC_DDS_PSL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def scrape_table_to_text():
    task_id = "scrape_table_to_text"
    log(task_id, "Starting scrape_table_to_text …")
    try:
        import hashlib
        from playwright.sync_api import sync_playwright
        def scrape_table_to_text_inner(page) -> str:
            rows = page.locator("table tr")
            row_count = rows.count()
            lines = []
            for i in range(row_count):
                cells = rows.nth(i).locator("th, td")
                cell_count = cells.count()
                row_text = "\t".join(
                    cells.nth(j).inner_text().strip() for j in range(cell_count)
                )
                if row_text.strip():
                    lines.append(row_text)
            return "\n".join(lines)
        def PRM_FL_DMS_CVL_scrape_table_to_text():
            base_url = "https://www.dms.myflorida.com/business_operations/state_purchasing/state_agency_resources/vendor_registration_and_vendor_lists"
            convicted_url = f"{base_url}/convicted_vendor_list"
            suspended_url = f"{base_url}/suspended_vendor_list"
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(convicted_url)
                page.wait_for_load_state("networkidle")
                convicted_text = scrape_table_to_text_inner(page)
                page.close()
                page = browser.new_page()
                page.goto(suspended_url)
                page.wait_for_load_state("networkidle")
                suspended_text = scrape_table_to_text_inner(page)
                page.close()
                browser.close()
            combined_text = convicted_text + suspended_text
            output_hash = hashlib.sha256(combined_text.encode("utf-8")).hexdigest()
            print(output_hash)
        _result = scrape_table_to_text_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "scrape_table_to_text", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "scrape_table_to_text", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_GA_DCH_EIE():
    task_id = "PRM_GA_DCH_EIE"
    log(task_id, "Starting PRM_GA_DCH_EIE …")
    try:
        import hashlib
        import time
        import requests
        from io import BytesIO
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_GA_DCH_EIE_inner():
            website = 'https://dch.georgia.gov/office-inspector-general/georgia-oig-exclusions-list'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, '//*[@id="main-content"]/div/div[3]/div[3]/div/main/p[6]/span/span[2]/a').get_attribute("href")
            print(Content)
            driver.quit()
            response = requests.get(Content)
            response.raise_for_status()
            output_hash = hashlib.sha256(response.content).hexdigest()
            print(output_hash)
        _result = PRM_GA_DCH_EIE_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_GA_DCH_EIE", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_GA_DCH_EIE", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_HI_DHS_PERL():
    task_id = "PRM_HI_DHS_PERL"
    log(task_id, "Starting PRM_HI_DHS_PERL …")
    try:
        import hashlib
        import time
        import requests
        from io import BytesIO
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_HI_DHS_PERL_inner():
            website = 'https://medquest.hawaii.gov/en/plans-providers/provider-exclusion-reinstatement-list.html'
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'List of Excluded Providers- Updated')]").text
            print(Content)
            driver.quit()
            output_hash = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_hash)
        _result = PRM_HI_DHS_PERL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_HI_DHS_PERL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_HI_DHS_PERL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_IA_DHS_MPSL():
    task_id = "PRM_IA_DHS_MPSL"
    log(task_id, "Starting PRM_IA_DHS_MPSL …")
    try:
        import hashlib
        import time
        import requests
        import io
        from io import BytesIO
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_IA_DHS_MPSL_inner():
            website = 'https://hhs.iowa.gov/medicaid/provider-services/excluded-individuals-and-entities'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Iowa Medicaid Sanction List')]").get_attribute("href")
            print(Content)
            driver.quit()
            response = requests.get(Content)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_IA_DHS_MPSL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_IA_DHS_MPSL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_IA_DHS_MPSL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_ID_DHW_MPEL():
    task_id = "PRM_ID_DHW_MPEL"
    log(task_id, "Starting PRM_ID_DHW_MPEL …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_ID_DHW_MPEL_inner():
            page = "https://healthandwelfare.idaho.gov/providers/idaho-medicaid-providers/information-medicaid-providers"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(10)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Medicaid Provider Exclusion List')]").get_attribute("href")
            print(Link)
            driver.close()
            driver = webdriver.Chrome(options=options)
            driver.get(Link)
            time.sleep(20)
            iframe = driver.find_element(By.XPATH, '//*[@id="pdfViewerIFrame"]')
            driver.switch_to.frame(iframe)
            Date = driver.find_element(By.XPATH, "//span[contains(text(),'Idaho Medicaid Exclusion List')]//following::span[2]").text
            print(Date)
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_ID_DHW_MPEL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_ID_DHW_MPEL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_ID_DHW_MPEL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_IL_DHFS_PSL():
    task_id = "PRM_IL_DHFS_PSL"
    log(task_id, "Starting PRM_IL_DHFS_PSL …")
    try:
        import hashlib
        import time
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_IL_DHFS_PSL_inner():
            website = 'https://ilhfspartner3.dynamics365portals.us/sanctions/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'  Sanctions Last Updated:')]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_IL_DHFS_PSL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_IL_DHFS_PSL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_IL_DHFS_PSL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_IN_FSSA_PT():
    task_id = "PRM_IN_FSSA_PT"
    log(task_id, "Starting PRM_IN_FSSA_PT …")
    try:
        import hashlib
        import time
        import requests
        import io
        from io import BytesIO
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_IN_FSSA_PT_inner():
            website = 'https://www.in.gov/fssa/ompp/provider-information4/termination-of-provider-participation-in-medicaid-and-chip/'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Terminated providers')]").get_attribute("href")
            print(Content)
            driver.quit()
            response = requests.get(Content)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_IN_FSSA_PT_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_IN_FSSA_PT", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_IN_FSSA_PT", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_KS_DHE_TPL():
    task_id = "PRM_KS_DHE_TPL"
    log(task_id, "Starting PRM_KS_DHE_TPL …")
    try:
        import hashlib
        import time
        import requests
        import io
        from io import BytesIO
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        def PRM_KS_DHE_TPL_inner():
            website = 'https://www.kdhe.ks.gov/172/Medicaid'
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(website)
            driver.maximize_window()
            #Cookies = driver.find_element(By.XPATH, "//*[contains(text(),'Accept all cookies')]").click()
            time.sleep(10)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Termination List (XLSX)')]").get_attribute("href")
            print(Content)
            driver.quit()
            response = requests.get(Content)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_KS_DHE_TPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_KS_DHE_TPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_KS_DHE_TPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_KY_CHFS_PT():
    task_id = "PRM_KY_CHFS_PT"
    log(task_id, "Starting PRM_KY_CHFS_PT …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_KY_CHFS_PT_inner():
            page = "https://www.chfs.ky.gov/agencies/dms/dpi/pe/Pages/terminated.aspx"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'Alphabetical Provider Termination List (Excel)')]")
            url = link.get_attribute("href")
            cookies = driver.get_cookies()
            ua = driver.execute_script("return navigator.userAgent;")
            driver.quit()
            print("Excel URL:", url)
            s = requests.Session()
            s.headers.update({
                "User-Agent": ua,
                "Referer": page
            })
            for c in cookies:
                s.cookies.set(c["name"], c["value"])
            r = s.get(url, timeout=60)
            r.raise_for_status()
            excel_bytes = r.content
            df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl")
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_KY_CHFS_PT_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_KY_CHFS_PT", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_KY_CHFS_PT", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_LA_DH_AAL():
    task_id = "PRM_LA_DH_AAL"
    log(task_id, "Starting PRM_LA_DH_AAL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_LA_DH_AAL_inner():
            page = "https://adverseactions.ldh.la.gov/SelSearch/SelSearch/Export"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'Download CSV')]")
            url = link.get_attribute("href")
            r = requests.get(url)
            r.raise_for_status()
            df = pd.read_csv(io.BytesIO(r.content), skiprows=1)
            Data = df.to_csv(index=False)
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_LA_DH_AAL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_LA_DH_AAL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_LA_DH_AAL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_LA_DOA_DV():
    task_id = "PRM_LA_DOA_DV"
    log(task_id, "Starting PRM_LA_DOA_DV …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_LA_DOA_DV_inner():
            page = "https://www.doa.la.gov/doa/osp/agency-resources/debarred-entities/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, '//*[@id="skip"]/div/div/div[2]/div/div[1]/div/div/table/tbody').text
            print(link)
            output_date = hashlib.sha256(link.encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_LA_DOA_DV_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_LA_DOA_DV", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_LA_DOA_DV", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MA_MH_SEMP():
    task_id = "PRM_MA_MH_SEMP"
    log(task_id, "Starting PRM_MA_MH_SEMP …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MA_MH_SEMP_inner():
            page = "https://www.mass.gov/info-details/learn-about-suspended-or-excluded-masshealth-providers"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, '//*[@id="main-content"]/div[2]/div/div/div[2]/section/div/ul/li[2]/div/span[2]/a').text
            print(link)
            output_date = hashlib.sha256(link.encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_MA_MH_SEMP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MA_MH_SEMP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MA_MH_SEMP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MD_BPW_CB():
    task_id = "PRM_MD_BPW_CB"
    log(task_id, "Starting PRM_MD_BPW_CB …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MD_BPW_CB_inner():
            page = "https://bpw.maryland.gov/Pages/Debarments.aspx"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, '//*[@id="ctl00_PlaceHolderMain_RichHtmlField1__ControlWrapper_RichHtmlField"]/table[1]').text
            print(link)
            output_date = hashlib.sha256(link.encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_MD_BPW_CB_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MD_BPW_CB", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MD_BPW_CB", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MD_MDH_MSPL():
    task_id = "PRM_MD_MDH_MSPL"
    log(task_id, "Starting PRM_MD_MDH_MSPL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MD_MDH_MSPL_inner():
            page = "https://health.maryland.gov/mmcp/provider/Pages/sanctioned_list.aspx"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, '//strong[contains(text(),"Maryland Medicaid")]//following::p[3]').text
            print(link)
            output_date = hashlib.sha256(link.encode('utf-8')).hexdigest()
            print(output_date)
        _result = PRM_MD_MDH_MSPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MD_MDH_MSPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MD_MDH_MSPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MI_DHHS_SPL():
    task_id = "PRM_MI_DHHS_SPL"
    log(task_id, "Starting PRM_MI_DHHS_SPL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MI_DHHS_SPL_inner():
            page = "https://www.michigan.gov/en/mdhhs/doing-business/providers/providers/billingreimbursement/list-of-sanctioned-providers"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, '//*[@id="ftn1"]/p[3]/strong[1]/strong/strong/strong/a').get_attribute("href")
            print(link)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                          "image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": page,  # IMPORTANT on some sites
                "Connection": "keep-alive",
            }
            response = requests.get(link, headers=headers, timeout=60, allow_redirects=True)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_MI_DHHS_SPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MI_DHHS_SPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MI_DHHS_SPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MO_DSS_PS():
    task_id = "PRM_MO_DSS_PS"
    log(task_id, "Starting PRM_MO_DSS_PS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MO_DSS_PS_inner():
            page = "https://mmac.mo.gov/providers/provider-sanctions/"
            pdf_path = "mo_suspended_debarred_vendors.pdf"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'List of Terminations:')]/following::a").get_attribute("href")
            print(link)
            driver.quit()
            driver = webdriver.Chrome(options=options)
            driver.get(link)
            time.sleep(8)
            Excel = driver.find_element(By.XPATH, '//*[@id="content"]/div/div[2]/div[2]/p/a').get_attribute("href")
            driver.quit()
            response = requests.get(Excel)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_MO_DSS_PS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MO_DSS_PS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MO_DSS_PS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MS_DM_SPL():
    task_id = "PRM_MS_DM_SPL"
    log(task_id, "Starting PRM_MS_DM_SPL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MS_DM_SPL_inner():
            page = "https://medicaid.ms.gov/providers/provider-terminations/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'Sanctioned Provider List')]").get_attribute("href")
            print(link)
            driver.quit()
            response = requests.get(link)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_MS_DM_SPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MS_DM_SPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MS_DM_SPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_MT_DPHHS_TMP():
    task_id = "PRM_MT_DPHHS_TMP"
    log(task_id, "Starting PRM_MT_DPHHS_TMP …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_MT_DPHHS_TMP_inner():
            page = "https://dphhs.mt.gov/MontanaHealthcarePrograms/TerminatedExcludedProviders"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, '//*[@id="main"]/div/div/div/table').text
            print(link)
            output_date = hashlib.sha256(link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_MT_DPHHS_TMP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_MT_DPHHS_TMP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_MT_DPHHS_TMP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NC_DA_DV():
    task_id = "PRM_NC_DA_DV"
    log(task_id, "Starting PRM_NC_DA_DV …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_NC_DA_DV_inner():
            page = "https://www.doa.nc.gov/divisions/purchase-contract/debarred-vendors"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'Files last revised:')]").text
            print(link)
            output_date = hashlib.sha256(link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_NC_DA_DV_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NC_DA_DV", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NC_DA_DV", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NC_DHHS_EPL():
    task_id = "PRM_NC_DHHS_EPL"
    log(task_id, "Starting PRM_NC_DHHS_EPL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_NC_DHHS_EPL_inner():
            page = "https://medicaid.ncdhhs.gov/providers/excluded-providers"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'State Excluded Provider List')]/ancestor::li[1]").text
            print(link)
            output_date = hashlib.sha256(link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_NC_DHHS_EPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NC_DHHS_EPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NC_DHHS_EPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_ND_DHS_MPEL():
    task_id = "PRM_ND_DHS_MPEL"
    log(task_id, "Starting PRM_ND_DHS_MPEL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_ND_DHS_MPEL_inner():
            page = "https://www.hhs.nd.gov/healthcare/medicaid/provider/compliance/fraud-and-abuse"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            link = driver.find_element(By.XPATH, "//*[contains(text(),'ND Medicaid Provider Exclusion List (xls)')]").get_attribute("href")
            print(link)
            driver.quit()
            response = requests.get(link)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_ND_DHS_MPEL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_ND_DHS_MPEL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_ND_DHS_MPEL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NH_DHHS_MPESL():
    task_id = "PRM_NH_DHHS_MPESL"
    log(task_id, "Starting PRM_NH_DHHS_MPESL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_NH_DHHS_MPESL_inner():
            page = "https://www.dhhs.nh.gov/programs-services/medicaid/medicaid-provider-relations"
            options = Options()
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            link = driver.find_element(
                By.XPATH,
                "//*[contains(text(),'Medicaid Provider Exclusion and Sanction List')]"
            ).get_attribute("href")
            print("Download URL:", link)
            driver.quit()
            session = requests.Session()
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel",
                "Referer": page,
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive"
            }
            session.get(page, headers=headers)
            response = session.get(link, headers=headers)
            response.raise_for_status()
            if "text/html" in response.headers.get("Content-Type", ""):
                raise Exception("Blocked – got HTML instead of Excel")
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(
                df.to_string(index=False).encode("utf-8")
            ).hexdigest()
            print(output_date)
        _result = PRM_NH_DHHS_MPESL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NH_DHHS_MPESL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NH_DHHS_MPESL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NJ_MFD_PER():
    task_id = "PRM_NJ_MFD_PER"
    log(task_id, "Starting PRM_NJ_MFD_PER …")
    try:
        import hashlib
        import requests
        from pypdf import PdfReader
        from io import BytesIO
        def PRM_NJ_MFD_PER_inner():
            url1="https://www.nj.gov/comptroller/doc/nj_debarment_list.pdf"
            response = requests.get(url1)
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_NJ_MFD_PER_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NJ_MFD_PER", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NJ_MFD_PER", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NV_DHHS_MSL():
    task_id = "PRM_NV_DHHS_MSL"
    log(task_id, "Starting PRM_NV_DHHS_MSL …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_NV_DHHS_MSL_inner():
            page = "https://www.nevadamedicaid.nv.gov/providers/provider-exclusions-sanctions-and-press-releases/"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'NV Exclusion List')]").get_attribute("href")
            print(Link)
            response = requests.get(Link)
            soup = BeautifulSoup(response.content, "html.parser")
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_NV_DHHS_MSL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NV_DHHS_MSL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NV_DHHS_MSL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NV_OLC_DC():
    task_id = "PRM_NV_OLC_DC"
    log(task_id, "Starting PRM_NV_OLC_DC …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_NV_OLC_DC_inner():
            page = "https://labor.nv.gov/PrevailingWage/Disqualified_Contractors/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'List of Disqualified Contractors')]//following::tbody[1]").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_NV_OLC_DC_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NV_OLC_DC", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NV_OLC_DC", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_NY_DOL_DL():
    task_id = "PRM_NY_DOL_DL"
    log(task_id, "Starting PRM_NY_DOL_DL …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def PRM_NY_DOL_DL_inner():
            page = "https://apps.labor.ny.gov/EDList/searchResults.do"
            options = Options()
            # options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            wait = WebDriverWait(driver, 15)
            driver.get(page)
            time.sleep(3)
            dol = driver.find_element(By.CSS_SELECTOR, "input[name='searchRadioBox'][value='dolSearch']")
            driver.execute_script("arguments[0].click();", dol)
            time.sleep(1)
            btn = driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value='Search']")
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(3)
            full_content = []
            while True:
                wait.until(EC.presence_of_element_located((By.ID, "ResultsTable")))
                table = driver.find_element(By.ID, "ResultsTable")
                Content = table.text.strip()
                full_content.append(Content)
                try:
                    next_btn = driver.find_element(By.LINK_TEXT, "Next")
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(2)
                except Exception:
                    print("[+] No more pages.")
                    break
            driver.quit()
            Content = "\n".join(full_content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_NY_DOL_DL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_NY_DOL_DL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_NY_DOL_DL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_OH_DM_MPESL():
    task_id = "PRM_OH_DM_MPESL"
    log(task_id, "Starting PRM_OH_DM_MPESL …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_OH_DM_MPESL_inner():
            page = "https://medicaid.ohio.gov/resources-for-providers/enrollment-and-support/provider-enrollment/provider-exclusion-and-suspension-list"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//a[contains(text(),'Ohio')]//following::strong").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_OH_DM_MPESL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_OH_DM_MPESL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_OH_DM_MPESL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_OR_BOLI_IC():
    task_id = "PRM_OR_BOLI_IC"
    log(task_id, "Starting PRM_OR_BOLI_IC …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_OR_BOLI_IC_inner():
            page = "https://www.oregon.gov/boli/employers/Pages/pwr-ineligible-contractors.aspx"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'List of Ineligibles')]/following::tbody").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_OR_BOLI_IC_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_OR_BOLI_IC", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_OR_BOLI_IC", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_PA_DGS_DSL():
    task_id = "PRM_PA_DGS_DSL"
    log(task_id, "Starting PRM_PA_DGS_DSL …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_PA_DGS_DSL_inner():
            page = "https://www.dgs.internet.state.pa.us/debarmentsearch/debarment/index"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Search = driver.find_element(By.XPATH, '//*[@id="btnOpenSearch"]').click()
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//div[table]").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_PA_DGS_DSL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_PA_DGS_DSL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_PA_DGS_DSL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_SC_DHHS_EPL():
    task_id = "PRM_SC_DHHS_EPL"
    log(task_id, "Starting PRM_SC_DHHS_EPL …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_SC_DHHS_EPL_inner():
            page = "https://www.scdhhs.gov/fraud-waste-and-abuse"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'South Carolina Medicaid Excluded/Terminated Providers')]").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_SC_DHHS_EPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_SC_DHHS_EPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_SC_DHHS_EPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_SC_SFAA_SD():
    task_id = "PRM_SC_SFAA_SD"
    log(task_id, "Starting PRM_SC_SFAA_SD …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_SC_SFAA_SD_inner():
            page = "https://procurement.sc.gov/legal/legal-suspend-debar"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//h3[contains(text(),'Suspensions & Debarments')]//ancestor::div[1]").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_SC_SFAA_SD_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_SC_SFAA_SD", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_SC_SFAA_SD", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_TN_OPI_TPL():
    task_id = "PRM_TN_OPI_TPL"
    log(task_id, "Starting PRM_TN_OPI_TPL …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_TN_OPI_TPL_inner():
            page = "https://www.tn.gov/tenncare/fraud-and-abuse/program-integrity.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Search = driver.find_element(By.XPATH, "//*[contains(text(),'Terminated Provider List')]").click()
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Terminated Provider List')]//following::table").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_TN_OPI_TPL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_TN_OPI_TPL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_TN_OPI_TPL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_TX_HHS_EP():
    task_id = "PRM_TX_HHS_EP"
    log(task_id, "Starting PRM_TX_HHS_EP …")
    try:
        import io, hashlib
        import requests
        import pandas as pd
        from bs4 import BeautifulSoup
        def PRM_TX_HHS_EP_inner():
            page = "https://oig.hhsc.state.tx.us/oigportal2/Exclusions/ctl/DOW/mid/384"
            EVENT_TARGET = "dnn$ctr384$DownloadExclusionsFile$lb_DLoad_ExcFile_XLS"
            s = requests.Session()
            s.headers.update({"User-Agent": "Mozilla/5.0"})
            r = s.get(page, timeout=60)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            payload = {}
            for inp in soup.select("input[type=hidden]"):
                name = inp.get("name")
                if name:
                    payload[name] = inp.get("value", "")
            payload["__EVENTTARGET"] = EVENT_TARGET
            payload["__EVENTARGUMENT"] = ""
            d = s.post(page, data=payload, timeout=120)
            d.raise_for_status()
            ct = (d.headers.get("Content-Type") or "").lower()
            if "text/html" in ct:
                raise RuntimeError("Got HTML back (postback failed). Inspect response text:\n" + d.text[:500])
            excel_bytes = d.content
            df = pd.read_excel(io.BytesIO(excel_bytes), engine="xlrd")  # pip install xlrd
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_TX_HHS_EP_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_TX_HHS_EP", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_TX_HHS_EP", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_TX_TC_DVL():
    task_id = "PRM_TX_TC_DVL"
    log(task_id, "Starting PRM_TX_TC_DVL …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_TX_TC_DVL_inner():
            page = "https://comptroller.texas.gov/purchasing/programs/vendor-performance-tracking/debarred-vendors.php"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//h1[contains(text(),'Debarred Vendor List')]//following::table").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_TX_TC_DVL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_TX_TC_DVL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_TX_TC_DVL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_WA_DOLI_DC():
    task_id = "PRM_WA_DOLI_DC"
    log(task_id, "Starting PRM_WA_DOLI_DC …")
    try:
        import time, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.common.exceptions import NoSuchElementException
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def PRM_WA_DOLI_DC_inner():
            page = "https://secure.lni.wa.gov/debarandstrike/ContractorDebarList.aspx"
            options = Options()
            # options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            wait = WebDriverWait(driver, 20)

            try:
                driver.get(page)
                time.sleep(6)

                def get_table_element():
                    return driver.find_element(By.XPATH, "//h1[contains(.,'Debarred Contractors List')]//following::table[1]")

                def get_last_page_number():
                    pager_nums = driver.find_elements(By.XPATH, "//a[normalize-space()=string(number(normalize-space()))]")
                    nums = []
                    for a in pager_nums:
                        txt = a.text.strip()
                        if txt.isdigit():
                            nums.append(int(txt))
                    return max(nums) if nums else 1

                all_pages_text = []
                seen_hashes = set()
                current_page = 1

                while True:
                    # ✅ Re-detect last page each time (will catch a new page 30 when it appears)
                    last_page = get_last_page_number()
                    # print("Detected last page:", last_page)
                    table_el = get_table_element()
                    table_text = table_el.text.strip()
                    page_hash = hashlib.sha256(table_text.encode("utf-8")).hexdigest()
                    if page_hash in seen_hashes:
                        print(f"\n[STOP] Repeated table detected again at page loop counter={current_page}.")
                        break
                    seen_hashes.add(page_hash)
                    all_pages_text.append(f"--- PAGE {current_page} ---\n{table_text}\n")
                    print(f"\n================= PAGE {current_page} =================")
                    print(table_text)
                    if current_page >= last_page:
                        print(f"\n[STOP] Reached last page ({last_page}).")
                        break
                    next_page_num = current_page + 1
                    try:
                        next_link = driver.find_element(By.XPATH, f"//a[normalize-space()='{next_page_num}']")
                    except NoSuchElementException:
                        try:
                            next_link = driver.find_element(By.XPATH, "//a[normalize-space()='Next']")
                        except NoSuchElementException:
                            print("\n[STOP] No Next / numeric pager link found.")
                            break
                    old_table = table_el
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_link)
                    time.sleep(0.3)
                    next_link.click()

                    try:
                        wait.until(EC.staleness_of(old_table))
                    except Exception:
                        old_hash = page_hash
                        try:
                            wait.until(lambda d: hashlib.sha256(get_table_element().text.strip().encode("utf-8")).hexdigest() != old_hash)
                        except Exception:
                            print("\n[STOP] Clicked next but grid did not refresh/change.")
                            break

                    time.sleep(0.8)
                    current_page += 1

                final_content = "\n".join(all_pages_text)
                output_date = hashlib.sha256(final_content.encode("utf-8")).hexdigest()
                print("\nSHA256:", output_date)

            finally:
                driver.quit()

        _result = PRM_WA_DOLI_DC_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_WA_DOLI_DC", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_WA_DOLI_DC", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_WA_HCA_PTEL():
    task_id = "PRM_WA_HCA_PTEL"
    log(task_id, "Starting PRM_WA_HCA_PTEL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_WA_HCA_PTEL_inner():
            page = "https://www.hca.wa.gov/billers-providers-partners/become-apple-health-provider/provider-termination-and-exclusion-list"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Download the HCA Medicaid')]").get_attribute("href")
            print(Content)
            driver.quit()
            response = requests.get(Content)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_WA_HCA_PTEL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_WA_HCA_PTEL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_WA_HCA_PTEL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_WI_DOT_DSIC():
    task_id = "PRM_WI_DOT_DSIC"
    log(task_id, "Starting PRM_WI_DOT_DSIC …")
    try:
        import hashlib
        import requests
        from pypdf import PdfReader
        from io import BytesIO
        def PRM_WI_DOT_DSIC_inner():
            url1="https://wisconsindot.gov/hccidocs/debar.pdf"
            response = requests.get(url1)
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
                print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_WI_DOT_DSIC_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_WI_DOT_DSIC", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_WI_DOT_DSIC", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_WV_DHHR_PSE():
    task_id = "PRM_WV_DHHR_PSE"
    log(task_id, "Starting PRM_WV_DHHR_PSE …")
    try:
        import time, hashlib, re
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        import urllib3
        def PRM_WV_DHHR_PSE_inner():
            page = "https://www.wvmmis.com/SitePages/Medicaid%20Provider%20SanctionedExclusion"
            base_url = "https://www.wvmmis.com"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            try:
                driver.get(page)
                time.sleep(8)
                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
                pdf_filename = None
                library_name = None
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if "doPostBack" in href and text.lower().endswith(".pdf"):
                        pdf_filename = text
                        tr = a.find_parent("tr")
                        if tr:
                            hidden_tds = tr.find_all("td", style=re.compile(r"display\s*:\s*none", re.I))
                            if hidden_tds:
                                library_name = hidden_tds[0].get_text(strip=True)
                        break
                if not pdf_filename:
                    raise ValueError("Could not locate PDF link on the page.")
                if not library_name:
                    library_name = "Medicaid+Provider+SanctionedExclusion"
                file_encoded = requests.utils.quote(pdf_filename)
                pdf_url = f"{base_url}/SharepointDownload?parent={library_name}&docname={file_encoded}"
                print("PDF URL:", pdf_url)
            finally:
                driver.quit()
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            }
            response = requests.get(pdf_url, headers=headers, timeout=60, verify=False)
            response.raise_for_status()
            if not response.content.startswith(b"%PDF"):
                preview = response.content[:300].decode("utf-8", errors="replace")
                raise ValueError("Downloaded content is not a PDF. Preview:\n" + preview)
            reader = PdfReader(BytesIO(response.content))
            full_text = ""
            for p in reader.pages:
                text = p.extract_text() or ""
                full_text += text.strip() + "\n"
            print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_WV_DHHR_PSE_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_WV_DHHR_PSE", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_WV_DHHR_PSE", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_WV_WVPD_DV():
    task_id = "PRM_WV_WVPD_DV"
    log(task_id, "Starting PRM_WV_WVPD_DV …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_WV_WVPD_DV_inner():
            page = "http://www.state.wv.us/admin/purchase/debar.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//*[contains(text(),'Vendor Name')]//ancestor::table[2]").text
            print(Content)
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_WV_WVPD_DV_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_WV_WVPD_DV", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_WV_WVPD_DV", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def PRM_WY_DH_PEL():
    task_id = "PRM_WY_DH_PEL"
    log(task_id, "Starting PRM_WY_DH_PEL …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def PRM_WY_DH_PEL_inner():
            page = "https://health.wyo.gov/healthcarefin/medicaid/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            iframe = driver.find_element(By.XPATH, '//*[@id="post-158"]/div/div/div/div/div[3]/div/div/div/div/div/p[3]/iframe')
            driver.switch_to.frame(iframe)
            Link = driver.find_element(By.XPATH, "//h3[contains(text(),'Wyoming Medicaid Provider Exclusion List')]//following::div").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = PRM_WY_DH_PEL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "PRM_WY_DH_PEL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "PRM_WY_DH_PEL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def QA_MOI_SANCTIONS_LIST():
    task_id = "QA_MOI_SANCTIONS_LIST"
    log(task_id, "Starting QA_MOI_SANCTIONS_LIST …")
    try:
        import time
        import hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def QA_MOI_SANCTIONS_LIST_inner():
            page = (
                "https://portal.moi.gov.qa/wps/portal/NCTC/sanctionlist/unifiedsanctionlist/"
                "!ut/p/z1/jZBBU4MwEIV_Ta5kBdpSbzTTISiUVumAuXRCQZoZSDAE_PsyVk8qdm_75r33zS5m"
                "OMdM8lHU3AgleTPtL2x5Cte2a1PXjhLi-nBwNmEaOzEEwQJnn4YFjbaB-wi7YJVu4JDYNF0-"
                "URvgDrNb8vDH-HBbfsbA5uszzK4IPw7BdiFKtmRqWNOUEgfA26--DHMn_gd5wKxuVHH9py8Lx"
                "6sx09VrpSttDXqSL8Z0_T0CBJ3ShjdWq4RVq9F64wjeu_5bR7AjKUEQV6XgJ1JJU2kEQpZiF"
                "OXAG3k25-eh-I1zUb3B-Y963LXHYw5i32Ze_wF7da29/dz/d5/L3dDZyEvUUZRSS9ZTlEh/"
            )
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            wait = WebDriverWait(driver, 20)
            time.sleep(8)
            all_text = []
            current_page = 1
            try:
                while True:
                    wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
                    time.sleep(2)
                    table = driver.find_element(By.XPATH, "//table")
                    page_text = table.text.strip()
                    if page_text and len(page_text) > 50:
                        all_text.append(page_text)
                        print(f"Scraped page {current_page}: {len(page_text)} chars")
                    else:
                        time.sleep(5)
                        page_text = driver.find_element(By.XPATH, "//table").text.strip()
                        if page_text:
                            all_text.append(page_text)
                            print(f"Scraped page {current_page} (delayed): {len(page_text)} chars")
                    try:
                        next_btn = driver.find_element(
                            By.XPATH,
                            "//li[contains(@class,'pagination-next') and not(contains(@class,'disabled'))]/a"
                        )
                        driver.execute_script("arguments[0].click();", next_btn)
                        current_page += 1
                        time.sleep(3)
                    except:
                        print(f"No enabled next button — finished at page {current_page}.")
                        break
            except Exception as e:
                print(f"Error on page {current_page}: {e}")
            finally:
                driver.quit()
            full_text = "\n".join(all_text)
            output_hash = hashlib.sha256(full_text.encode('utf-8')).hexdigest()
            print(output_hash)
            return output_hash
        _result = QA_MOI_SANCTIONS_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "QA_MOI_SANCTIONS_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "QA_MOI_SANCTIONS_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def SECO_SWISS_GUATEMALA():
    task_id = "SECO_SWISS_GUATEMALA"
    log(task_id, "Starting SECO_SWISS_GUATEMALA …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def SECO_SWISS_GUATEMALA_inner():
            page = "https://www.seco.admin.ch/seco/de/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos/sanktionsmassnahmen/massnahmen-gegenueber-guatemala.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//*[contains(text(),'Änderung PDF')]//following::td[2]").text
            print(Date)
            driver.quit()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = SECO_SWISS_GUATEMALA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "SECO_SWISS_GUATEMALA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "SECO_SWISS_GUATEMALA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def SECO_SWISS_HAMAS():
    task_id = "SECO_SWISS_HAMAS"
    log(task_id, "Starting SECO_SWISS_HAMAS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def SECO_SWISS_HAMAS_inner():
            page = "https://www.seco.admin.ch/seco/de/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos/sanktionsmassnahmen/massnahmen-gegenueber-hamas.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//*[contains(text(),'Änderung PDF')]//following::td[2]").text
            print(Date)
            driver.quit()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = SECO_SWISS_HAMAS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "SECO_SWISS_HAMAS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "SECO_SWISS_HAMAS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def SECO_SWISS_MOLDOVA():
    task_id = "SECO_SWISS_MOLDOVA"
    log(task_id, "Starting SECO_SWISS_MOLDOVA …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def SECO_SWISS_MOLDOVA_inner():
            page = "https://www.seco.admin.ch/seco/de/home/Aussenwirtschaftspolitik_Wirtschaftliche_Zusammenarbeit/Wirtschaftsbeziehungen/exportkontrollen-und-sanktionen/sanktionen-embargos/sanktionsmassnahmen/massnahmen-gegenueber-moldau.html"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Änderung XML')]//following::td[3]").text
            print(Link)
            driver.close()
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = SECO_SWISS_MOLDOVA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "SECO_SWISS_MOLDOVA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "SECO_SWISS_MOLDOVA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def SG_ACRA_SUSPENSION():
    task_id = "SG_ACRA_SUSPENSION"
    log(task_id, "Starting SG_ACRA_SUSPENSION …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def SG_ACRA_SUSPENSION_inner():
            page = "https://www.polis.gov.bn/orang-dikehendaki/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link2 = driver.find_element(By.XPATH, '//*[@id="post-4184"]/div/div/div/div/div').text
            print(Link2)
            output_date = hashlib.sha256(Link2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = SG_ACRA_SUSPENSION_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "SG_ACRA_SUSPENSION", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "SG_ACRA_SUSPENSION", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def SG_MAS_RUSSIA():
    task_id = "SG_MAS_RUSSIA"
    log(task_id, "Starting SG_MAS_RUSSIA …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def SG_MAS_RUSSIA_inner():
            page = "https://www.mas.gov.sg/regulation/anti-money-laundering/targeted-financial-sanctions"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//*[contains(text(),'Financial measures in relation to Russia')]").click()
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//*[contains(text(),'Amendment Notes')]//following::p[1]").text
            print(Data)
            driver.quit()
            output_date = hashlib.sha256(Data.encode("utf-8")).hexdigest()
            print(output_date)
        _result = SG_MAS_RUSSIA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "SG_MAS_RUSSIA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "SG_MAS_RUSSIA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def SG_SGX_WATCH_LIST():
    task_id = "SG_SGX_WATCH_LIST"
    log(task_id, "Starting SG_SGX_WATCH_LIST …")
    try:
        import time, hashlib
        import requests
        from io import BytesIO
        from bs4 import BeautifulSoup
        from pypdf import PdfReader
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def SG_SGX_WATCH_LIST_inner():
            page = "https://regco.sgx.com/directors-and-executive-officers-watchlist"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Link = driver.find_element(By.XPATH, "//h1[contains(text(),'Watchlist')]//following::table").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = SG_SGX_WATCH_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "SG_SGX_WATCH_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "SG_SGX_WATCH_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TH_AMLO_SANCTIONS():
    task_id = "TH_AMLO_SANCTIONS"
    log(task_id, "Starting TH_AMLO_SANCTIONS …")
    try:
        import re
        import hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def TH_AMLO_SANCTIONS_inner():
            start_url = "https://www.amlo.go.th/index.php/en/#"
            options = Options()
            #options.add_argument("--headless=new")  # enable if you want faster headless runs
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.page_load_strategy = "eager"
            options.add_experimental_option(
                "prefs",
                {"profile.managed_default_content_settings.images": 2}
            )
            driver = webdriver.Chrome(options=options)
            wait = WebDriverWait(driver, 25)
            try:
                driver.get(start_url)
                wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/form"))).click()
                wait.until(EC.element_to_be_clickable((By.ID, "menu_desktop545"))).click()
                section7 = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//a[contains(.,'List of designated persons under Section 7')]")
                    )
                )
                section7_url = section7.get_attribute("href")
                driver.get(section7_url)
                body_text = wait.until(EC.presence_of_element_located((By.TAG_NAME, "body"))).text
                m = re.search(
                    r"(List of Designated Persons\s*\(Updated as of\s*\d{1,2}\s+[A-Za-z]+\s+\d{4}\))",
                    body_text
                )
                if not m:
                    raise RuntimeError("Could not find the exact heading: 'List of Designated Persons (Updated as of ...)'")
                heading = m.group(1).strip()
                heading_hash = hashlib.sha256(heading.encode("utf-8")).hexdigest()
                print(heading)
                print(heading_hash)
            finally:
                driver.quit()
        _result = TH_AMLO_SANCTIONS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TH_AMLO_SANCTIONS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TH_AMLO_SANCTIONS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TREAS_FINCEN_ADVISORY():
    task_id = "TREAS_FINCEN_ADVISORY"
    log(task_id, "Starting TREAS_FINCEN_ADVISORY …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TREAS_FINCEN_ADVISORY_inner():
            page = "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets/advisories"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Page2 = driver.find_element(By.XPATH, "//table//tr[1]//td[2]").text
            print(Page2)
            output_date = hashlib.sha256(Page2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TREAS_FINCEN_ADVISORY_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TREAS_FINCEN_ADVISORY", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TREAS_FINCEN_ADVISORY", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TREAS_FINCEN_PMLC():
    task_id = "TREAS_FINCEN_PMLC"
    log(task_id, "Starting TREAS_FINCEN_PMLC …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TREAS_FINCEN_PMLC_inner():
            page = "https://www.fincen.gov/resources/statutes-and-regulations/special-measures"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Page2 = driver.find_element(By.XPATH, "//table").text
            print(Page2)
            output_date = hashlib.sha256(Page2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TREAS_FINCEN_PMLC_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TREAS_FINCEN_PMLC", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TREAS_FINCEN_PMLC", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TR_MTF_ARTICLE_3():
    task_id = "TR_MTF_ARTICLE_3"
    log(task_id, "Starting TR_MTF_ARTICLE_3 …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TR_MTF_ARTICLE_3_inner():
            page = "https://en.hmb.gov.tr/3a3b"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link2 = driver.find_element(By.XPATH, "//*[contains(text(),'Excel')]").get_attribute("href")
            print(Link2)
            driver.close()
            response = requests.get(Link2)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = TR_MTF_ARTICLE_3_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TR_MTF_ARTICLE_3", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TR_MTF_ARTICLE_3", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TR_MTF_ARTICLE_5():
    task_id = "TR_MTF_ARTICLE_5"
    log(task_id, "Starting TR_MTF_ARTICLE_5 …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TR_MTF_ARTICLE_5_inner():
            page = "https://masak.hmb.gov.tr/5-maddeye-iliskin-bakanlar-kurulu-kararlari"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//*[contains(text(),'Resmi Gazete')]//following::td[1]").text
            print(Date)
            driver.quit()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TR_MTF_ARTICLE_5_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TR_MTF_ARTICLE_5", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TR_MTF_ARTICLE_5", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TR_MTF_ARTICLE_6():
    task_id = "TR_MTF_ARTICLE_6"
    log(task_id, "Starting TR_MTF_ARTICLE_6 …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TR_MTF_ARTICLE_6_inner():
            page = "https://masak.hmb.gov.tr/6-maddeye-iliskin-bakanlar-kurulu-kararlari"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//*[contains(text(),'Resmi Gazete')]//following::td[1]").text
            print(Date)
            driver.quit()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TR_MTF_ARTICLE_6_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TR_MTF_ARTICLE_6", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TR_MTF_ARTICLE_6", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TR_MTF_ARTICLE_7():
    task_id = "TR_MTF_ARTICLE_7"
    log(task_id, "Starting TR_MTF_ARTICLE_7 …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TR_MTF_ARTICLE_7_inner():
            page = "https://masak.hmb.gov.tr/7madde"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//*[contains(text(),'Resmi Gazete')]//following::td[2]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = TR_MTF_ARTICLE_7_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TR_MTF_ARTICLE_7", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TR_MTF_ARTICLE_7", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TT_FIU_DPRK():
    task_id = "TT_FIU_DPRK"
    log(task_id, "Starting TT_FIU_DPRK …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TT_FIU_DPRK_inner():
            page = "https://agla.gov.tt/anti-terrorism-unit/atu-proliferation-financing-of-weapons-of-mass-destruction/atu-proliferation-financing-of-weapons-of-mass-destruction/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[2]/div[1]/article/div[2]/div/div').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TT_FIU_DPRK_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TT_FIU_DPRK", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TT_FIU_DPRK", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TT_FIU_HAITI():
    task_id = "TT_FIU_HAITI"
    log(task_id, "Starting TT_FIU_HAITI …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TT_FIU_HAITI_inner():
            page = "https://agla.gov.tt/uncategorized/united-nations-security-council-resolution-unscr-26532022-haiti/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="panel-26470-0-0-0"]/div/div/div').text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TT_FIU_HAITI_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TT_FIU_HAITI", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TT_FIU_HAITI", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TW_CBI_MW():
    task_id = "TW_CBI_MW"
    log(task_id, "Starting TW_CBI_MW …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TW_CBI_MW_inner():
            page = "https://www.cib.npa.gov.tw/en/app/globalcase/list?module=globalcase&id=2160"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Most Wanted List')]//following::div").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TW_CBI_MW_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TW_CBI_MW", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TW_CBI_MW", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def TW_FSC_EA():
    task_id = "TW_FSC_EA"
    log(task_id, "Starting TW_FSC_EA …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def TW_FSC_EA_inner():
            page = "https://www.banking.gov.tw/en/home.jsp?id=93&parentpath=0,86"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, "//div[contains(text(),'Topic')]//following::div[5]").text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = TW_FSC_EA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "TW_FSC_EA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "TW_FSC_EA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def UA_SSU_WANTED():
    task_id = "UA_SSU_WANTED"
    log(task_id, "Starting UA_SSU_WANTED …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def UA_SSU_WANTED_inner():
            page = "https://ssu.gov.ua/en/u-rozshuku"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//h2[contains(text(),'Wanted')]//following::ul[2]").text
            print(Link)
            driver.close()
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = UA_SSU_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "UA_SSU_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "UA_SSU_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_AF_FUGITIVES():
    task_id = "US_AF_FUGITIVES"
    log(task_id, "Starting US_AF_FUGITIVES …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_AF_FUGITIVES_inner():
            page = "https://www.osi.af.mil/AFOSI-Wanted/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'fugitives')]//following::div").text
            print(Link)
            output_date = hashlib.sha256(Link.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_AF_FUGITIVES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_AF_FUGITIVES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_AF_FUGITIVES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_CBP_UEL():
    task_id = "US_CBP_UEL"
    log(task_id, "Starting US_CBP_UEL …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_CBP_UEL_inner():
            page = "https://www.dhs.gov/uflpa-entity-list"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8)
            Content = driver.find_element(By.XPATH, '//*[@id="block-mainpagecontent"]/article').text
            print(Content)
            driver.quit()
            output_date = hashlib.sha256(Content.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_CBP_UEL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_CBP_UEL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_CBP_UEL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def get_next_button():
    task_id = "get_next_button"
    log(task_id, "Starting get_next_button …")
    try:
        import time, hashlib
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from bs4 import BeautifulSoup
        def get_next_button_inner(driver):
            selectors = [
                # ── aria-label variants ──────────────────────────────────────────────
                (By.CSS_SELECTOR, "[aria-label='Go to next page']"),
                (By.CSS_SELECTOR, "[aria-label='Next page']"),
                (By.CSS_SELECTOR, "[aria-label='Next Page']"),
                (By.CSS_SELECTOR, "[aria-label='next']"),
                (By.CSS_SELECTOR, "[aria-label='Next']"),
                # ── ServiceNow list / SP widget classes ──────────────────────────────
                (By.CSS_SELECTOR, ".sn-widget-list-pager .next"),
                (By.CSS_SELECTOR, ".sn-widget-list-pager [data-direction='next']"),
                (By.CSS_SELECTOR, "[data-direction='next']"),
                (By.CSS_SELECTOR, "[data-action='next_page']"),
                (By.CSS_SELECTOR, ".sp-pagination .next a"),
                (By.CSS_SELECTOR, ".sp-pagination li.next a"),
                # ── Bootstrap / generic pagination ───────────────────────────────────
                (By.CSS_SELECTOR, ".pagination li.next a"),
                (By.CSS_SELECTOR, ".pagination li.next button"),
                (By.CSS_SELECTOR, ".pagination .page-item.next .page-link"),
                (By.CSS_SELECTOR, "li.next > a"),
                (By.CSS_SELECTOR, "li.next > button"),
                # ── Icon classes ─────────────────────────────────────────────────────
                (By.CSS_SELECTOR, "button[class*='next']"),
                (By.CSS_SELECTOR, "a[class*='next']"),
                (By.CSS_SELECTOR, "button[class*='forward']"),
                (By.CSS_SELECTOR, "[class*='pagination'][class*='next']"),
                # ── Plain text > ─────────────────────────────────────────────────────
                (By.XPATH, "//button[normalize-space(text())='>']"),
                (By.XPATH, "//a[normalize-space(text())='>']"),
                # ── Buttons with > inside span/icon ──────────────────────────────────
                (By.XPATH, "//button[.//span[normalize-space(text())='>']]"),
                (By.XPATH, "//a[.//span[normalize-space(text())='>']]"),
                # ── Buttons containing right-arrow icon spans ─────────────────────────
                (By.XPATH, "//button[.//*[contains(@class,'right') or contains(@class,'next') or contains(@class,'arrow')]]"),
            ]
            for by, sel in selectors:
                try:
                    els = driver.find_elements(by, sel)
                    for el in els:
                        if el.is_displayed():
                            return el, sel
                except Exception:
                    pass
            return None, None
        def dump_pagination_html_get_next_button(driver):
            """Print all visible buttons + pagination containers for debugging."""
            print("\n" + "="*60)
            print("DEBUG: Could not find Next button. Dumping page info...")
            print("="*60)
            html = driver.execute_script("""
                var results = [];
                var els = document.querySelectorAll('button, a, [role="button"]');
                els.forEach(function(el) {
                    if (el.offsetParent !== null || el.offsetWidth > 0) {
                        results.push(el.outerHTML);
                    }
                });
                return results.join('\\n---\\n');
            """)
            print("VISIBLE BUTTONS / LINKS:\n")
            print(html[:5000] if html else "(none)")
            pag = driver.execute_script("""
                var results = [];
                var els = document.querySelectorAll('*');
                els.forEach(function(el) {
                    var cls = (el.className || '').toString().toLowerCase();
                    var id  = (el.id || '').toLowerCase();
                    if (cls.includes('paginat') || cls.includes('pager') || id.includes('paginat')) {
                        results.push(el.outerHTML);
                    }
                });
                return results.join('\\n---\\n');
            """)
            print("\nPAGINATION CONTAINERS:\n")
            print(pag[:3000] if pag else "(none — try looking in the buttons above)")
            print("="*60 + "\n")
        def US_DDTC_POA_get_next_button():
            page = "https://www.pmddtc.state.gov/ddtc_public?id=ddtc_kb_article_page&sys_id=384b968adb3cd30044f9ff621f961941"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(8) 
            all_rows = []
            headers = []
            current_page = 1
            while True:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
                time.sleep(2)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.find("table")
                if not headers:
                    headers = [th.get_text(strip=True) for th in table.find_all("th")]
                    print(f"Headers: {headers}")
                tbody = table.find("tbody")
                trs = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
                page_rows = 0
                for tr in trs:
                    cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all("td")]
                    if cells:
                        if headers and len(cells) == len(headers):
                            all_rows.append(dict(zip(headers, cells)))
                        else:
                            all_rows.append({f"col_{i}": v for i, v in enumerate(cells)})
                        page_rows += 1
                print(f"Page {current_page}: {page_rows} rows (total so far: {len(all_rows)})")
                next_btn, matched_sel = get_next_button_inner(driver)
                if not next_btn:
                    dump_pagination_html_get_next_button(driver)
                    print("Stopping — paste the DEBUG output above and share it to get the exact fix.")
                    break
                print(f"  Next button matched: {matched_sel}")
                is_disabled = driver.execute_script("""
                    var el = arguments[0];
                    return el.disabled === true
                        || el.getAttribute('disabled') !== null
                        || el.getAttribute('aria-disabled') === 'true'
                        || (el.className || '').includes('disabled')
                        || ((el.parentElement && el.parentElement.className) || '').includes('disabled');
                """, next_btn)
                if is_disabled:
                    print("Next button is disabled — reached last page.")
                    break
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", next_btn)
                    current_page += 1
                    time.sleep(3)
                except Exception as e:
                    print(f"Click failed: {e}")
                    break
            driver.quit()
            df = pd.DataFrame(all_rows)
            print(f"\nTotal rows collected: {len(df)}")
            print(df.to_string(index=False))
            output_hash = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_hash)
        _result = get_next_button_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "get_next_button", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "get_next_button", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_DEA_MOST_WANTED():
    task_id = "US_DEA_MOST_WANTED"
    log(task_id, "Starting US_DEA_MOST_WANTED …")
    try:
        import time, hashlib, re
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from bs4 import BeautifulSoup
        def US_DEA_MOST_WANTED_inner():
            BASE_URL = "https://www.dea.gov/fugitives/all"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(BASE_URL)
            time.sleep(5)
            all_rows = []
            soup = BeautifulSoup(driver.page_source, "html.parser")
            last_page_tag = soup.find("a", title=lambda t: t and "last page" in t.lower())
            if last_page_tag:
                match = re.search(r"page=(\d+)", last_page_tag["href"])
                last_page = int(match.group(1)) if match else 0
            else:
                last_page = 0
            total_pages = last_page + 1  # pages are 0-indexed (?page=0 ... ?page=54)
            print(f"Total pages: {total_pages}")
            for page_num in range(total_pages):
                url = BASE_URL if page_num == 0 else f"{BASE_URL}?page={page_num}"
                if page_num > 0:
                    driver.get(url)
                    time.sleep(2)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h3 a[href*='/fugitives/']"))
                )
                page_soup = BeautifulSoup(driver.page_source, "html.parser")
                fugitive_links = page_soup.select("h3 a[href*='/fugitives/']")
                page_rows = 0
                for tag in fugitive_links:
                    name = tag.get_text(strip=True)
                    href = tag["href"]
                    profile_url = "https://www.dea.gov" + href if href.startswith("/") else href
                    parent = tag.find_parent(["article", "div", "li", "section"])
                    charges = ""
                    if parent:
                        p_tag = parent.find("p")
                        if p_tag:
                            charges = p_tag.get_text(separator=" ", strip=True)
                    img_tag = parent.find("img") if parent else None
                    photo_url = ""
                    if img_tag and img_tag.get("src"):
                        src = img_tag["src"]
                        photo_url = "https://www.dea.gov" + src if src.startswith("/") else src
                    all_rows.append({
                        "name":        name,
                        "charges":     charges,
                        "profile_url": profile_url,
                        "photo_url":   photo_url,
                    })
                    page_rows += 1
                print(f"Page {page_num + 1}/{total_pages}: {page_rows} fugitives (total: {len(all_rows)})")
            driver.quit()
            df = pd.DataFrame(all_rows)
            print(f"\nTotal fugitives collected: {len(df)}")
            print(df.to_string(index=False))
            output_hash = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_hash)
        _result = US_DEA_MOST_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_DEA_MOST_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_DEA_MOST_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_DHS_BFV():
    task_id = "US_DHS_BFV"
    log(task_id, "Starting US_DHS_BFV …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_DHS_BFV_inner():
            page = "https://www.dco.uscg.mil/Our-Organization/Assistant-Commandant-for-Prevention-Policy-CG-5P/Inspections-Compliance-CG-5PC-/Commercial-Vessel-Compliance/Foreign-Offshore-Compliance-Division/Port-State-Control/BannedVessels/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Page2 = driver.find_element(By.XPATH, "//*[contains(text(),'Vessels Banned From')]//ancestor::table").text
            print(Page2)
            output_date = hashlib.sha256(Page2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_DHS_BFV_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_DHS_BFV", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_DHS_BFV", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_DOT_MOST_WANTED():
    task_id = "US_DOT_MOST_WANTED"
    log(task_id, "Starting US_DOT_MOST_WANTED …")
    try:
        import time, hashlib, re
        import requests
        import pandas as pd
        from bs4 import BeautifulSoup
        def US_DOT_MOST_WANTED_inner():
            BASE_URL = "https://www.oig.dot.gov/wanted-fugitives"
            HEADERS = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            session = requests.Session()
            all_rows = []
            def parse_page(soup):
                rows = []
                for li in soup.select("ul.item-list li, .view-content li, article li, li"):
                    links = li.find_all("a", href=re.compile(r"^/wanted-fugitives/[^?#]+$"))
                    for a in links:
                        # Skip the photo link (it wraps an <img>)
                        if a.find("img"):
                            continue
                        name = a.get_text(strip=True)
                        if name:
                            rows.append(name)
                return rows
            resp = session.get(BASE_URL, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            last_link = soup.find("a", string=re.compile(r"last", re.I))
            if last_link and "page=" in last_link.get("href", ""):
                last_page = int(re.search(r"page=(\d+)", last_link["href"]).group(1))
            else:
                page_nums = [
                    int(m.group(1))
                    for a in soup.select("nav.pager a, ul.pager a, .pagination a, a[href*='page=']")
                    for m in [re.search(r"page=(\d+)", a.get("href", ""))]
                    if m
                ]
                last_page = max(page_nums) if page_nums else 0

            total_pages = last_page + 1
            print(f"Total pages detected: {total_pages}")
            for page_num in range(total_pages):
                if page_num == 0:
                    page_soup = soup
                else:
                    time.sleep(1)
                    url = f"{BASE_URL}?page={page_num}"
                    r = session.get(url, headers=HEADERS, timeout=30)
                    r.raise_for_status()
                    page_soup = BeautifulSoup(r.text, "html.parser")
                names = parse_page(page_soup)
                seen = set()
                unique_names = []
                for n in names:
                    if n not in seen:
                        seen.add(n)
                        unique_names.append(n)
                for name in unique_names:
                    name_hash = hashlib.sha256(name.encode("utf-8")).hexdigest()
                    all_rows.append({"name": name, "name_hash": name_hash})
                print(f"Page {page_num + 1}/{total_pages}: {len(unique_names)} fugitives "
                      f"(total: {len(all_rows)})")
            df = pd.DataFrame(all_rows).drop_duplicates(subset="name").reset_index(drop=True)
            print(f"\nTotal unique fugitives: {len(df)}")
            print(df.to_string(index=False))
            output_hash = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_hash)
        _result = US_DOT_MOST_WANTED_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_DOT_MOST_WANTED", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_DOT_MOST_WANTED", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_EPA_FUGITIVES():
    task_id = "US_EPA_FUGITIVES"
    log(task_id, "Starting US_EPA_FUGITIVES …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_EPA_FUGITIVES_inner():
            page = "https://www.epa.gov/enforcement/epa-fugitives"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Page2 = driver.find_element(By.XPATH, '//*[@id="main"]/div').text
            print(Page2)
            output_date = hashlib.sha256(Page2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_EPA_FUGITIVES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_EPA_FUGITIVES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_EPA_FUGITIVES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_FDA_CI():
    task_id = "US_FDA_CI"
    log(task_id, "Starting US_FDA_CI …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_FDA_CI_inner():
            page = "https://www.accessdata.fda.gov/scripts/SDA/sdNavigation.cfm?sd=clinicalinvestigatorsdisqualificationproceedings&previewMode=true&displayAll=true"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Page2 = driver.find_element(By.XPATH, '//*[@id="user_provided"]/table/tbody/tr/td/table/tbody/tr[6]').text
            print(Page2)
            output_date = hashlib.sha256(Page2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_FDA_CI_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_FDA_CI", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_FDA_CI", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_FDA_WL():
    task_id = "US_FDA_WL"
    log(task_id, "Starting US_FDA_WL …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_FDA_WL_inner():
            page = "https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations/compliance-actions-and-activities/warning-letters?search_api_fulltext=&search_api_fulltext_issuing_office=&field_letter_issue_datetime=All&field_change_date_closeout_letter=&field_change_date_response_letter=&field_change_date_2=All&field_letter_issue_datetime_2=&export=yes"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Data = driver.find_element(By.XPATH, "//table//tr[1]//td[1]").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = US_FDA_WL_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_FDA_WL", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_FDA_WL", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_FD_OCI_MOST_WANTED_FUGITIVES():
    task_id = "US_FD_OCI_MOST_WANTED_FUGITIVES"
    log(task_id, "Starting US_FD_OCI_MOST_WANTED_FUGITIVES …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_FD_OCI_MOST_WANTED_FUGITIVES_inner():
            page = "https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations/news-resources/ocis-most-wanted"
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Page2 = driver.find_element(By.XPATH, "//h3[contains(text(),'Most Wanted Fugitives')]//following::div[2]").text
            print(Page2)
            output_date = hashlib.sha256(Page2.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_FD_OCI_MOST_WANTED_FUGITIVES_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_FD_OCI_MOST_WANTED_FUGITIVES", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_FD_OCI_MOST_WANTED_FUGITIVES", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_NCDST_IFD_LIST():
    task_id = "US_NCDST_IFD_LIST"
    log(task_id, "Starting US_NCDST_IFD_LIST …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from pypdf import PdfReader
        from bs4 import BeautifulSoup
        from io import BytesIO
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.devtools.v143.dom import get_attributes
        def US_NCDST_IFD_LIST_inner():
            page = "https://www.nctreasurer.gov/about/transparency/divestment-and-do-not-contract-rules#IranDivestmentandDo-Not-ContractResources-546"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, '//*[@id="Tab-IranDivestmentandDo-Not-ContractResources-546"]/div/div/p[4]/a').get_attribute("href")
            print(Link)
            driver.close()
            response = requests.get(Link)
            soup = BeautifulSoup(response.content, "html.parser")
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_NCDST_IFD_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_NCDST_IFD_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_NCDST_IFD_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_NJL_IDA():
    task_id = "US_NJL_IDA"
    log(task_id, "Starting US_NJL_IDA …")
    try:
        import hashlib
        import requests
        from pypdf import PdfReader
        from io import BytesIO
        def US_NJL_IDA_inner():
            url1="https://www.nj.gov/treasury/doinvest/pdf/index/Iran_Progress_Report.pdf"
            response = requests.get(url1)
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
                print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_NJL_IDA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_NJL_IDA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_NJL_IDA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_OGT_SCI():
    task_id = "US_OGT_SCI"
    log(task_id, "Starting US_OGT_SCI …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_OGT_SCI_inner():
            page = "https://data.treasury.ri.gov/ne/dataset/scrutinzed-companies-list-iran"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Link = driver.find_element(By.XPATH, "//*[contains(text(),'Rhode_Island_Iran')]").click()
            time.sleep(5)
            Link2 = driver.find_element(By.XPATH, "//h1[contains(text(),'Rhode_Island_Iran')]//following::a").get_attribute("href")
            print(Link2)
            driver.close()
            response = requests.get(Link2)
            response.raise_for_status()
            excel_bytes = response.content
            df = pd.read_excel(io.BytesIO(excel_bytes))
            print(df.to_string(index=False))
            output_date = hashlib.sha256(df.to_string(index=False).encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_OGT_SCI_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_OGT_SCI", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_OGT_SCI", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_PDGS_IFP_LIST():
    task_id = "US_PDGS_IFP_LIST"
    log(task_id, "Starting US_PDGS_IFP_LIST …")
    try:
        import hashlib
        import requests
        from pypdf import PdfReader
        from io import BytesIO
        def US_PDGS_IFP_LIST_inner():
            url1="https://www.pa.gov/content/dam/copapwp-pagov/en/dgs/documents/documents/procurement-forms/proposediranfreeprocurementlist.pdf"
            response = requests.get(url1)
            response.raise_for_status()
            pdf_bytes = response.content
            pdf_file = BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)
            full_text = ""
            for page in reader.pages:
                text = page.extract_text() or ""
                full_text += text.strip()
                print(full_text)
            output_date = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_PDGS_IFP_LIST_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_PDGS_IFP_LIST", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_PDGS_IFP_LIST", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def scroll_page():
    task_id = "scroll_page"
    log(task_id, "Starting scroll_page …")
    try:
        import time
        import io
        import hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        def scroll_page_inner(driver):
            for scroll_y in range(0, 8000, 400):
                driver.execute_script(f"window.scrollTo(0, {scroll_y});")
                time.sleep(0.2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
        def extract_cards_scroll_page(driver):
            records = []
            cards = driver.find_elements(By.CLASS_NAME, "jet-listing-grid__item")
            print(f"  [INFO] Found {len(cards)} cards on this page.")
            for card in cards:
                try:
                    h2_els = card.find_elements(By.CSS_SELECTOR, "h2.elementor-heading-title")
                    category = h2_els[0].text.strip() if len(h2_els) > 0 else ""
                    name     = h2_els[1].text.strip() if len(h2_els) > 1 else ""
                    try:
                        region = card.find_element(
                            By.CSS_SELECTOR, "span.jet-listing-dynamic-terms__link"
                        ).text.strip()
                    except Exception:
                        region = ""
                    try:
                        reward = card.find_element(
                            By.CSS_SELECTOR, "p.elementor-heading-title"
                        ).text.strip()
                    except Exception:
                        reward = ""
                    try:
                        url = card.find_element(
                            By.CSS_SELECTOR, "a.jet-engine-listing-overlay-link"
                        ).get_attribute("href")
                    except Exception:
                        url = ""
                    if name:
                        records.append({
                            "name":     name,
                            "category": category,
                            "region":   region,
                            "reward":   reward,
                            "url":      url,
                        })
                except Exception as e:
                    print(f"  [WARN] Card parse error: {e}")
            return records
        def get_next_button_scroll_page(driver):
            try:
                next_btn = driver.find_element(
                    By.XPATH,
                    "//*[contains(@class,'jet-filters-pagination__item') and "
                    "(normalize-space(.)='Next' or normalize-space(.)='»')] | "
                    "//a[contains(@class,'jet-filters-pagination__item') and "
                    "(normalize-space(.)='Next' or normalize-space(.)='»')]"
                )
                if next_btn.is_displayed() and next_btn.is_enabled():
                    return next_btn
            except Exception:
                pass
            try:
                next_btn = driver.find_element(
                    By.XPATH,
                    "//*[contains(translate(@class,'NEXT','next'),'next') and "
                    "not(contains(@class,'prev'))]//a | "
                    "//a[contains(translate(@class,'NEXT','next'),'next') and "
                    "not(contains(@class,'prev'))]"
                )
                if next_btn.is_displayed() and next_btn.is_enabled():
                    return next_btn
            except Exception:
                pass
            return None
        def US_RFJ_WT_scroll_page():
            page = "https://rewardsforjustice.net/index/"
            options = Options()
            # options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            print("  [INFO] Waiting for page to fully load ...")
            time.sleep(8)
            scroll_page_inner(driver)
            all_records = []
            current_page = 1
            while True:
                print(f"\n{'='*60}")
                print(f"  Processing page {current_page} ...")
                print(f"{'='*60}")
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "jet-listing-grid__items"))
                )
                time.sleep(2)
                scroll_page_inner(driver)
                page_records = extract_cards_scroll_page(driver)
                all_records.extend(page_records)
                print(f"\n  Extracted {len(page_records)} records from page {current_page}:")
                for r in page_records:
                    print(f"    • {r['name']}")
                    print(f"      Category : {r['category']}")
                    print(f"      Region   : {r['region']}")
                    print(f"      Reward   : {r['reward']}")
                    print(f"      URL      : {r['url']}")
                next_btn = get_next_button_scroll_page(driver)
                if next_btn:
                    print(f"\n  [INFO] Clicking Next → page {current_page + 1} ...")
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", next_btn)
                    current_page += 1
                    time.sleep(5)  # Wait for next page to load
                else:
                    print(f"\n  [INFO] No Next button found. Reached last page ({current_page}).")
                    break
            driver.close()
            print(f"\n{'='*60}")
            print(f"  TOTAL INDIVIDUALS EXTRACTED: {len(all_records)} across {current_page} pages")
            print(f"{'='*60}\n")
            lines = []
            for i, r in enumerate(all_records, 1):
                lines.append(f"{i}. {r['name']}")
                lines.append(f"   Category : {r['category']}")
                lines.append(f"   Region   : {r['region']}")
                lines.append(f"   Reward   : {r['reward']}")
                lines.append(f"   URL      : {r['url']}")
                lines.append("")
            output_text = "\n".join(lines)
            print(output_text)
            output_hash = hashlib.sha256(output_text.encode("utf-8")).hexdigest()
            print(output_hash)
        _result = scroll_page_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "scroll_page", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "scroll_page", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_SEC_LR():
    task_id = "US_SEC_LR"
    log(task_id, "Starting US_SEC_LR …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_SEC_LR_inner():
            page = "https://www.sec.gov/enforcement-litigation/litigation-releases"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//*[contains(text(),'Date')]//following::td[1]").text
            print(Date)
            driver.close()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_SEC_LR_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_SEC_LR", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_SEC_LR", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def US_SS_MWF():
    task_id = "US_SS_MWF"
    log(task_id, "Starting US_SS_MWF …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def US_SS_MWF_inner():
            page = "https://www.secretservice.gov/investigations/mostwanted"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//h1[contains(text(),'Most Wanted Fugitives')]//following::div[2]").text
            print(Date)
            driver.close()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = US_SS_MWF_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "US_SS_MWF", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "US_SS_MWF", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def ZA_FIC_SANCTIONS():
    task_id = "ZA_FIC_SANCTIONS"
    log(task_id, "Starting ZA_FIC_SANCTIONS …")
    try:
        import time, io, hashlib
        import requests
        import pandas as pd
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def ZA_FIC_SANCTIONS_inner():
            page = "https://www.fic.gov.za/compliance/supervision-and-enforcement/sanctions-issued-by-the-fic/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(5)
            Date = driver.find_element(By.XPATH, "//h2[contains(text(),'Sanctions issued by the FIC')]//following::td").text
            print(Date)
            driver.close()
            output_date = hashlib.sha256(Date.encode("utf-8")).hexdigest()
            print(output_date)
        _result = ZA_FIC_SANCTIONS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "ZA_FIC_SANCTIONS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "ZA_FIC_SANCTIONS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def ZA_FIC_TFS():
    task_id = "ZA_FIC_TFS"
    log(task_id, "Starting ZA_FIC_TFS …")
    try:
        import io
        import hashlib
        import requests
        import pandas as pd
        def ZA_FIC_TFS_inner() -> str:
            base_url  = "https://tfs.fic.gov.za"
            page_url  = f"{base_url}/Pages/TFSListDownload"
            post_url  = f"{base_url}/Pages/TFSListDownload?fileType=excel"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
                "Referer":    page_url,
                "Origin":     base_url,
            }
            session = requests.Session()
            session.get(page_url, headers=headers, timeout=30)
            response = session.post(
                post_url,
                headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
                data={},
                timeout=60,
                allow_redirects=True,
            )
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "")
            print(f"[+] Response Content-Type : {content_type}")
            print(f"[+] Response size         : {len(response.content):,} bytes")
            if "html" in content_type.lower():
                raise RuntimeError(
                    "Server returned HTML instead of an Excel file. "
                    "The POST may need additional form fields or cookies. "
                    "Check the DevTools Network tab for the exact request payload."
                )
            df = pd.read_excel(io.BytesIO(response.content))
            print(f"[+] DataFrame shape       : {df.shape}")
            print(df.to_string(index=False))
            df_string   = df.to_string(index=False)
            sha256_hash = hashlib.sha256(df_string.encode("utf-8")).hexdigest()
            print(sha256_hash)
            return sha256_hash
        if False:
            ZA_FIC_TFS_inner()
        _result = ZA_FIC_TFS_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "ZA_FIC_TFS", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "ZA_FIC_TFS", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ────────────────────────────────────────────────────────────────────
def ZA_FSCA_EA():
    task_id = "ZA_FSCA_EA"
    log(task_id, "Starting ZA_FSCA_EA …")
    try:
        import time, io, hashlib
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        def ZA_FSCA_EA_inner():
            page = "https://www.fsca.co.za/Enforcement-Actions/"
            options = Options()
            #options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            driver.get(page)
            time.sleep(10)
            iframe = driver.find_element(By.XPATH, '//*[@id="myFrame"]')
            driver.switch_to.frame(iframe)
            Data = driver.find_element(By.XPATH, "//strong[contains(text(),'Enforcement Actions Search')]//following::table").text
            print(Data)
            output_date = hashlib.sha256(Data.encode('utf-8')).hexdigest()
            print(output_date)
        _result = ZA_FSCA_EA_inner()
        log(task_id, "Completed successfully", "success")
        return {"task": "ZA_FSCA_EA", "status": "success", "result": _result}
    except Exception as _exc:
        import traceback as _tb
        log(task_id, f"FAILED – {_exc}", "error")
        return {"task": "ZA_FSCA_EA", "status": "error", "error": str(_exc), "traceback": _tb.format_exc()}


# ── Task registry ────────────────────────────────────────────────────────────
ALL_TASKS = [
    AR_MOJ_RePET_E,
    AR_MOJ_RePET_P,
    AR_NIS_CPL,
    BD_POLICE_WANTED,
    BN_BDCB_AL,
    CA_GC_NRO,
    CA_SPEC_MEAS_ESV,
    CA_SPEC_MEAS_GUATEMALA,
    CA_SPEC_MEAS_HAMAS_TA,
    CA_SPEC_MEAS_MOLDOVA,
    CA_SPEC_MEAS_SRILANKA,
    CA_SPEC_MEAS_SUDAN,
    CH_UN_COTE_DIVOIRE_1572,
    CI_ADB_DEBARRED_ENTITIES,
    CT_DOL_DL,
    CT_DSS_AAL,
    DK_DIS_ENTRY_BAN,
    DK_IS_PROHIBITED_ENTITIES,
    DK_IS_PROHIBITED_PERSONS,
    DOC_BIS_RL,
    DOS_ADP,
    DOS_CRL,
    DOS_SDP,
    DOS_TEL,
    EE_FSA_IA,
    EE_MFA_BELARUS,
    EE_MFA_HR,
    EE_MFA_RUSSIA,
    ES_BOS_SANCTIONS,
    ES_NPC_TERRORISTS,
    ES_WA_CNMV,
    EU_EBRD_INELIGIBLE_ENTITIES,
    EU_ECB_SANCTIONS,
    EU_EC_EDES,
    EU_EIB_LOE,
    EU_EUROPOL_MWF,
    FBI_LAW_ENFORCEMENT,
    FBI_MWT,
    FBI_SI,
    FBI_TOP_10,
    GB_FCA_UCB,
    GB_OFSI_TR_MP,
    GE_MOJ_OTKHOZORIA_TATUNASHVILI_LIST,
    GG_GFSC_DD,
    HK_ICAC_WL,
    HU_MNB_MSW,
    IE_CBI_PN,
    IL_IIPB_PIL,
    IL_MOF_WMD,
    IL_MOJ_TERR_ORG,
    IM_FSA_DD,
    IM_FSA_ENFORCEMENT,
    INTERPOL_UN_Notices_Entities,
    INTERPOL_UN_Notices_Individuals,
    IN_BANNED_ORG,
    IN_SEBI_DEBARRED_ENTITIES,
    IN_UNLAWFUL_ASSOCIATION,
    JP_METI,
    JP_UN_CAR_2134,
    JP_UN_CONGO,
    JP_UN_SANCTIONS,
    JP_UN_SANCTIONS_IRAN,
    JP_UN_SANCTIONS_SOMALIA,
    JP_UN_SANCTIONS_SYRIA,
    JP_UN_SS_2206,
    JP_UN_YE_2140,
    KE_FRC_DTFS,
    KG_FIU_SANCTIONS_ENTITIES,
    KG_FIU_SANCTIONS_INDIVIDUALS,
    KR_MFA_SAKP,
    LB_ISF_NTFL,
    LV_FIU_SANCTIONED_SUBJECTS,
    MC_BT_NAFL,
    MN_OSP_SDVR,
    MT_MPF_WANTED,
    MY_BNM_WANTED,
    MY_BNM_WARNING_LETTERS,
    MY_MHA_MOHA_LIST,
    MY_SCM_IA,
    MY_SCM_WANTED,
    NG_EFCC_WANTED_PERSON,
    NG_NIGSAC_ENTITIES,
    NG_NIGSAC_INDIVIDUALS,
    NO_BLACK_LIST,
    NY_OGS_IDA,
    OIG_MOST_WANTED,
    OM_NCTC_LL,
    PA_TREAS_SCI,
    PE_MEF_DISQUALIFIED_SUPPLIERS,
    PL_MOF_SRM,
    PRM_AK_DHSS_EPL,
    PRM_AL_AMA_SP,
    PRM_AR_DHS_EP,
    PRM_CA_DHCS_SIPL,
    PRM_CA_DIR_DC,
    PRM_DC_DDS_PSL,
    scrape_table_to_text,
    PRM_GA_DCH_EIE,
    PRM_HI_DHS_PERL,
    PRM_IA_DHS_MPSL,
    PRM_ID_DHW_MPEL,
    PRM_IL_DHFS_PSL,
    PRM_IN_FSSA_PT,
    PRM_KS_DHE_TPL,
    PRM_KY_CHFS_PT,
    PRM_LA_DH_AAL,
    PRM_LA_DOA_DV,
    PRM_MA_MH_SEMP,
    PRM_MD_BPW_CB,
    PRM_MD_MDH_MSPL,
    PRM_MI_DHHS_SPL,
    PRM_MO_DSS_PS,
    PRM_MS_DM_SPL,
    PRM_MT_DPHHS_TMP,
    PRM_NC_DA_DV,
    PRM_NC_DHHS_EPL,
    PRM_ND_DHS_MPEL,
    PRM_NH_DHHS_MPESL,
    PRM_NJ_MFD_PER,
    PRM_NV_DHHS_MSL,
    PRM_NV_OLC_DC,
    PRM_NY_DOL_DL,
    PRM_OH_DM_MPESL,
    PRM_OR_BOLI_IC,
    PRM_PA_DGS_DSL,
    PRM_SC_DHHS_EPL,
    PRM_SC_SFAA_SD,
    PRM_TN_OPI_TPL,
    PRM_TX_HHS_EP,
    PRM_TX_TC_DVL,
    PRM_WA_DOLI_DC,
    PRM_WA_HCA_PTEL,
    PRM_WI_DOT_DSIC,
    PRM_WV_DHHR_PSE,
    PRM_WV_WVPD_DV,
    PRM_WY_DH_PEL,
    QA_MOI_SANCTIONS_LIST,
    SECO_SWISS_GUATEMALA,
    SECO_SWISS_HAMAS,
    SECO_SWISS_MOLDOVA,
    SG_ACRA_SUSPENSION,
    SG_MAS_RUSSIA,
    SG_SGX_WATCH_LIST,
    TH_AMLO_SANCTIONS,
    TREAS_FINCEN_ADVISORY,
    TREAS_FINCEN_PMLC,
    TR_MTF_ARTICLE_3,
    TR_MTF_ARTICLE_5,
    TR_MTF_ARTICLE_6,
    TR_MTF_ARTICLE_7,
    TT_FIU_DPRK,
    TT_FIU_HAITI,
    TW_CBI_MW,
    TW_FSC_EA,
    UA_SSU_WANTED,
    US_AF_FUGITIVES,
    US_CBP_UEL,
    get_next_button,
    US_DEA_MOST_WANTED,
    US_DHS_BFV,
    US_DOT_MOST_WANTED,
    US_EPA_FUGITIVES,
    US_FDA_CI,
    US_FDA_WL,
    US_FD_OCI_MOST_WANTED_FUGITIVES,
    US_NCDST_IFD_LIST,
    US_NJL_IDA,
    US_OGT_SCI,
    US_PDGS_IFP_LIST,
    scroll_page,
    US_SEC_LR,
    US_SS_MWF,
    ZA_FIC_SANCTIONS,
    ZA_FIC_TFS,
    ZA_FSCA_EA,
]


# ── Runner ────────────────────────────────────────────────────────────────────
def run_all_parallel(tasks, max_workers=20):
    results = {}

    print(f"\n{COLORS['BOLD']}{COLORS['CYAN']}{{'\u2550'*62}}")
    print(f"  PARALLEL SCRAPER AGENT  \u2013  {len(tasks)} tasks starting concurrently")
    print(f"{{'\u2550'*62}}{COLORS['RESET']}\n")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_fn = {executor.submit(fn): fn.__name__ for fn in tasks}
        for future in as_completed(future_to_fn):
            fn_name = future_to_fn[future]
            try:
                results[fn_name] = future.result()
            except Exception as exc:
                results[fn_name] = {"task": fn_name, "status": "error", "error": str(exc)}

    elapsed = time.time() - start_time

    print(f"\n{COLORS['BOLD']}{COLORS['CYAN']}{{'\u2550'*62}}")
    print(f"  SUMMARY  (completed in {elapsed:.1f}s)")
    print(f"{{'\u2550'*62}}{COLORS['RESET']}")

    ok   = {k: v for k, v in results.items() if v.get("status") == "success"}
    fail = {k: v for k, v in results.items() if v.get("status") != "success"}

    for fn_name, res in sorted(ok.items()):
        print(f"{COLORS['BOLD']}  \u2714  {fn_name}{COLORS['RESET']}")
        r = res.get("result")
        if isinstance(r, dict):
            h = r.get("hash") or r.get("output_hash", "")
            if h:
                print(f"       hash   : {h}")

    if fail:
        print()
    for fn_name, res in sorted(fail.items()):
        print(f"{COLORS['RED']}{COLORS['BOLD']}  \u2718  {fn_name}{COLORS['RESET']}")
        first_line = str(res.get("error", "unknown")).splitlines()[0][:120]
        print(f"       error  : {first_line}")

    print(f"\n  Total: {len(ok)}/{len(results)} succeeded\n")
    return results


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel Scraper Agent — 175 tasks")
    parser.add_argument("--workers", "-w", type=int, default=20,
                        help="Max parallel workers (default: 20)")
    parser.add_argument("--filter",  "-f", default=None,
                        help="Run only tasks whose name contains this string")
    parser.add_argument("--task",    "-t", default=None,
                        help="Run exactly one named task")
    parser.add_argument("--list",    "-l", action="store_true",
                        help="List all task names and exit")
    args = parser.parse_args()

    tasks = ALL_TASKS[:]
    pattern = (args.filter or args.task or "").lower()
    if pattern:
        tasks = [fn for fn in tasks if pattern in fn.__name__.lower()]

    if not tasks:
        print(f"{COLORS['RED']}No tasks matched.{COLORS['RESET']}")
        sys.exit(1)

    if args.list:
        print(f"{COLORS['BOLD']}Discovered {len(tasks)} task(s):{COLORS['RESET']}")
        for i, fn in enumerate(tasks, 1):
            print(f"  {i:>3}. {fn.__name__}")
        sys.exit(0)

    run_all_parallel(tasks, max_workers=args.workers)
