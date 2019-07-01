from flask import Flask, request, render_template
import requests
import pandas as pd

app = Flask(__name__)

url = 'http://localhost:3000/75549315857'

@app.route('/')
def mainPageHandler():
    return 'nothing here' 

@app.route('/75549315857')
def loanIdHandler():
    
    #customer_details = pd.read_sql_query('SELECT * FROM [dbo].[view_FCL_Loan] WHERE Loanid = 75549315857')
    return 'received loanid..preparing data'
    print(customer_details)
    #requests.post(url,json={'text': 'very good'})
    
    
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port= 8090)