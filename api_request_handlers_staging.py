import sys
sys.path.insert(0,'/home/danc/amogh')
import api_utility_staging
from flask import jsonify
import pandas as pd

reload(api_utility)


df_lender_vars = pd.DataFrame()
 

def accounts_handler(loanid):
    """
    Account Number fetch requests are handled here 
    
    Parameters:
    loanid(str)
    
    
    Returns:
    JSON : bank account number/s of a customer in JSON format
    
    """
 
    df_all_info=api_utility.fetch_all_info(loanid)
    if 'Error' in  df_all_info:
        return jsonify(df_all_info)
    parsed_cust_bank_report = api_utility.parse_cust_bank_report(df_all_info['BankReportData'][0])
    cust_acct_details = api_utility.get_cust_acc_info(parsed_cust_bank_report)
    send_acc_nos_to_dispatcher(cust_acct_details)
    lender_vars_handler(loanid)
    

def send_acc_nos_to_dispatcher(cust_acct_details):
    return jsonify(cust_acct_details)


def lender_vars_handler(loanid):
    df_all_info = api_utility.fetch_all_info(loanid)
    if 'Error' in  df_all_info:
        return jsonify(df_all_info)
    parsed_cust_bank_report = api_utility.parse_cust_bank_report(df_all_info['BankReportData'][0])
    parsed_cust_bank_report['TimeAdded'] = df_all_info['TimeAdded'][0]
    all_txns = api_utility.get_all_txns(parsed_cust_bank_report)
    all_txns = api_utility.pull_lender_vars(all_txns)
    df_lender_vars = create_lender_vars(all_txns)
    

def send_lender_vars(acc_no):
    return jsonify(df_lender_vars[df_lender_vars['accountNumber']==acc_no])
    
    
    


