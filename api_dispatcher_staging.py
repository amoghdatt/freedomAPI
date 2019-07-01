from flask import Flask,request,jsonify
import pandas as pd
import sys
sys.path.insert(0,'/home/danc/amogh')
import api_request_handlers_staging
reload(api_request_handlers_staging)



app = Flask(__name__)

dispatcher = {
    'account_fetch':api_request_handlers.accounts_handler,
    'lender_vars_fetch':api_request_handlers.send_lender_vars
}

@app.route('/fetchvariables',methods=['POST'])
def request_dispatcher():
    data = request.get_json()
    loanid = data['loanid']
    request_type = data['request_type']
    requested_info = dispatcher[request_type](str(loanid))
    return requested_info


if __name__ == '__main__':
    app.run(host='0.0.0.0',port= 8091,debug=True)