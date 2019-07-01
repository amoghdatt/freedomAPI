import pandas as pd
import numpy as np
import sys
sys.path.insert(0,'/home/danc/dattada/feature_repo')
import db_connect
import json


def parse_cust_bank_report(json_string):
    """
    Parse bank report of the customer
    
    Parameters:
    json_string(json): json containing bank report
    
    Returns:
    dictionary: containing parsed bank report details.
    
    """
    parsed_cust_bank_report = json.loads(json_string)
    
    return parsed_cust_bank_report
  
      
                                                 
def fetch_all_info(loanid):
    """
    Fetch bank report of the customer with a given loanid from the database
    
    Parameters:
    loanid(str): loanid of the customer 
    
    Returns:
    JSON: bank report of the customer in JSON format 
    
    """
    customer_accounts = dict()
    query = """
    SELECT GC.*
    FROM [FreedomCashLenders].[dbo].[view_FCL_GetCreditData]  GC
    LEFT JOIN [dbo].[view_FCL_GetCreditDataLoan] GCL ON GC.CustomerId = GCL.CustomerId 
    WHERE ReportStatus = 'COMPLETE' AND LoanId = """ + "'" + loanid + "'"
    
    df_all_info = pd.read_sql_query(query,con=db_connect.mssql_connect())
    
    if df_all_info.empty:
        return({'Error':'Please check the LoanID'})
    
    
    return df_all_info
    #or row in df_bank_report_data.itertuples():
    #   customer_bank_report=row[1]
    #return customer_bank_report



def get_cust_acc_info(parsed_bank_report):
    
    """
    Gets customer bank account number/s from the bank report
    
    Parameters:
    parsed_bank_report: Bank report of the customer 
    
    Returns:
    dictionary: dictionary containing bank account number/s in the form {'accountNumbers':[list of bank account numbers]}
    
    """
    accts_list = list()
    accts_dict = dict()
    for accts in parsed_bank_report["accounts"]:
        acct_details_dict = dict()
        if ('transactions' in accts.keys()) and (len(accts['transactions'])>0) and (accts['accountNumber'] not in accts_list) and (accts['accountType']=='checking'):
            acct_details_dict = accts.copy()
            accts_list.append(accts['accountNumber'])
            accts_dict['accountNumbers'] = accts_list
            
    return accts_dict  


def get_all_txns(parsed_bank_report):

    accts_list = list()
    for accts in parsed_bank_report['accounts']:
        #all_accounts.append(accts['accountNickname'] + '-'+ accts['accountNumber'])
        
        if ('transactions' in accts.keys()) and (len(accts['transactions'])>0) and (accts['accountNumber'] not in accts_list) and (accts['accountType']=='checking'):
            
            df_txn_temp = pd.DataFrame(accts['transactions'])
            df_txn_temp['account_number'] = accts['accountNumber']
            df_txn = df_txn.append(df_txn_temp,ignore_index = True)
            accts_list.append(accts['accountNumber'])
    return df_txn


def parse_dates(json_date):
    """
    Converts json formatted date to pandas datetime 
    
    Parameters:
    json_date
    
    Returns:
    pandas datetime
    """
    return datetime.fromtimestamp(int(json_date)/1000.0).strftime('%Y-%m-%d')


def pull_lender_vars(all_txns):
    df_lender_txn = pd.DataFrame()
    all_txns['lender_flag'] = all_txns['memo'].str.contains('|'.join(lend_cos),case = False,na=False)
    all_txns['category'] = all_txns['contexts'].map(lambda x: x[0]['categoryName'] if len(x) > 0 else np.nan)
    all_txns['posted_date'] = all_txns['postedDate'].map(parse_dates)
    all_txns['posted_date'] = pd.to_datetime(all_txns['posted_date'])
    condn = (all_txns['lender_flag'] == 1) & (all_txns['account_type'] == 'checking')
    req_cols = ['amount','balance','category','LoanId','TimeAdded','posted_date','type','account_number']
    all_txns = all_txns.loc[condn,req_cols]
    df_lender_txn = pd.concat([df_lender_txn,all_txns],ignore_index=True,sort = True)
    return df_lender_txn
    
    
def create_lender_vars(df):
    df_lender_txn = pull_lender_txn(df)
    count_cols = ['lender_count_debit','lender_count_credit','lender_count_debit_30','lender_count_credit_30']
    amount_cols =['lender_amount_debit','lender_amount_credit','lender_amount_debit_30','lender_amount_credit_30']
    
    for col in count_cols:
        df_lender_txn[col]=0
    for col in amount_cols:
        df_lender_txn[col]=df_lender_txn['amount']
    
    cond1 = ((df_lender_txn['type'].isin(['debit','directDebit','fee','pointOfSale','check','atm','cash','serviceCharge','payment']) | df_lender_txn['category'].isin(['Loan Payment'])) & (df_lender_txn['amount']<0))
    cond2 = ((df_lender_txn['type'] != 'fee') & (df_lender_txn['amount'] > 0))
    cond3 = ((df_lender_txn['type'].isin(['debit','directDebit','fee','pointOfSale','check','atm','cash','serviceCharge','payment']) | df_lender_txn['category'].isin(['Loan Payment'])) & ((df_lender_txn['amount']<0) & ((df_lender_txn['TimeAdded']-df_lender_txn['posted_date']).dt.days<=30)))
    cond4 = (((df_lender_txn['type'] != 'fee') & (df_lender_txn['amount'] > 0)) & (((df_lender_txn['TimeAdded']-df_lender_txn['posted_date']).dt.days<=30)))
    
    df_lender_txn.loc[cond1,'lender_count_debit'] = 1
    df_lender_txn.loc[cond2,'lender_count_credit'] = 1
    df_lender_txn.loc[~cond1,'lender_amount_debit'] = 0
    df_lender_txn.loc[~cond2,'lender_amount_credit'] = 0
    df_lender_txn.loc[cond3,'lender_count_debit_30'] = 1
    df_lender_txn.loc[cond4,'lender_count_credit_30'] = 1
    df_lender_txn.loc[~cond3,'lender_amount_debit_30'] = 0
    df_lender_txn.loc[~cond4,'lender_amount_credit_30'] = 0
    
    req_cols = count_cols + amount_cols + ['account_number']
    
    df_lender_vars = df_lender_txn[req_cols].groupby(['account_number'], as_index = False).sum()