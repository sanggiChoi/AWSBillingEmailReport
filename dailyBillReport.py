#!/usr/bin/python
# coding: UTF8

import sys
from datetime import datetime
import os.path

import boto.s3.connection

import boto.ses.connection

from decimal import Decimal

import csv

import locale
locale.setlocale(locale.LC_ALL, '')

import platform
import ConfigParser
import ast

billingDir              = '/opt/dailyBillReport/billData'

ACCOUNT_COLUMN          = 2;
PRODUCT_COLUMN          = 12;
EXPENSE_COLUMN          = 28;

def connectS3Profile(region, credentials_profile=None):
    
    conn = None
    try:
        conn = boto.s3.connect_to_region(region,
                                         profile_name=credentials_profile,
                                         is_secure=True,               # uncommmnt if you are not using ssl
                                         calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                                        )
    except Exception, e:
        sys.stderr.write ('Could not connect to region: %s. Exception: %s\n' % (region, e))
        conn = None
        sys.exit()
    return conn


def getBucket(conn, bucketName):
    bucket = None
    try:
        bucket = conn.get_bucket(bucketName, validate=False)
    except Exception, e:
        sys.stderr.write ('Could not get bucket: %s. Exception: %s\n' % (bucketName, e))
        bucket = None
        sys.exit()
    return bucket

def retrieve_fd(region, credentials_profile, bucket_name, fn, tmp_dir='.'):
    
    remote_fn = 's3://%s/%s' % (bucket_name, fn)
    print 'remote fn:', remote_fn
    
    conn    = connectS3Profile(region, credentials_profile)
    bucket  = getBucket(conn, bucket_name)
    key     = bucket.get_key(fn)
    
    if not key:
        raise Exception('remote file not ready : %s' % remote_fn)
    try:
        key.get_contents_to_filename(os.path.join(tmp_dir, fn))
    except Exception as e:
        print e
        return False
    
    return True
    
def removeBillingZip(filePath):
    
    if os.path.isfile(filePath):
        os.remove(filePath)

def isEmpty(obj):
    if(obj == None):
        return True
    
    strObj = str(obj)
    
    if(strObj):
        if(strObj == '' or len(strObj) == 0):
            return True;
    else:
        return True
    
    return False

def generateHtmlReport(totalCost, productionCost, testbedCost, accountID, linkedAccountID, currency):
    body = """\
<html>
  <head>AWS Usage Report</head>
  <body>"""
  
    body += '<p>최근 %s AWS 이용료를 알려드립니다.</p>' % accountID

    for key,value in sorted(totalCost.iteritems(), key=lambda (k,v): (v, k), reverse=True):
        if key is 'Total' and value is not str('0.0'):
            body  += '<b><p>%s: %s %s</p></b>\n' % (str('Total').encode('utf_8'), str(locale.format('%.2f', value, 1)), currency)
    
    body += '<p>자세한 내용은 다음과 같습니다.</p>\n'
    body += '<table border="1" cellspacing="0" cellpadding="10">\n'
    body += '<tr>'
    body += '<td>Servive Name</td>'
    body += '<td >Total</td><td>%s</td><td>%s</td>' % (accountID, linkedAccountID)
    body += '</tr>'
    # 세부 내용
    for key,value in sorted(totalCost.iteritems(), key=lambda (k,v): (v, k), reverse=True):
        #if key is not 'Total':
            body += '<tr>'
            body += '<td>%s</td>\n' % (key.encode('utf_8'))
            
            body += '<td align=right>%s %s</td>\n' % (str(locale.format('%.2f', value, 1)), currency)
            
            try:
                body += '<td align=right>%s %s</td>\n' % (str(locale.format('%.2f', productionCost[key], 1)), currency) 
            except KeyError:
                body += '<td align=right>%s %s</td>\n' % ('0.00', currency)
                
            try:
                body += '<td align=right>%s %s</td>\n' % (str(locale.format('%.2f', testbedCost[key], 1)), currency)
            except:
                body += '<td align=right>%s %s</td>\n' % ('0.00', currency)
                
            body += '</tr>'
 
    body += '</table>\n'
    body += """\
</body>
</html>
"""
    return body

def send_mail_by_ses(from_address, to_address, cc_address, reply_address, subject, body, credentials_profile=None):
    conn = boto.ses.connection.SESConnection(profile_name=credentials_profile)
    conn.send_email(from_address,
                   subject,
                   body,
                   to_address,
                   cc_address,
                   format='html',
                   reply_addresses=reply_address)

def makeBillData(row, result):
    
    if result.get(row[PRODUCT_COLUMN]) != None:
        currentExpense = result[row[PRODUCT_COLUMN]];
    else:
        currentExpense = 0
                    
    if (isEmpty(currentExpense)):
        currentExpense = 0         
         
    newExpenseString = row[EXPENSE_COLUMN];
    newExpense = 0
    if isEmpty(newExpenseString):
        newExpense = 0;
    else:
        newExpense = newExpenseString;
 
    if isEmpty(row[PRODUCT_COLUMN]) == False:
        value1 = Decimal(currentExpense)
        value2 = Decimal(newExpense)
        finalExpense = round(value1,2) + round(value2, 2)
        if finalExpense > 0:
            result[row[PRODUCT_COLUMN]] = finalExpense
    
def printBillData(result):
    totalCost = 0
    
    for key,value in sorted(result.iteritems(), key=lambda (k,v): (v, k), reverse=True):
        print '%-20s: %.2f USD' % (key, value)
        totalCost += result[key]
    
    result['Total'] = totalCost    
    
def reportBill(filePath, accountID, linkedAccountID):
    
    ifile = open(filePath, 'rb')
    reader = csv.reader(ifile)
    
    payAccountResult = {}
    linkedAccount1 = {}
    linkedAccount2 = {}
    
    for row in reader:
        if row[ACCOUNT_COLUMN] == accountID:
            makeBillData(row, linkedAccount1)
        elif row[ACCOUNT_COLUMN] == linkedAccountID:
            makeBillData(row, linkedAccount2)
        elif len(row[ACCOUNT_COLUMN]) == 0:
            makeBillData(row, payAccountResult)
                
    ifile.close()
    
    return payAccountResult, linkedAccount1, linkedAccount2

def generateReport(detailedBillingFile, linked_account1, linked_account2, currency='USD'):
    
    totalCost, productionCost, testbedCost = reportBill(os.path.join(billingDir, detailedBillingFile), linked_account1, linked_account2)
    
    print ''
    printBillData(totalCost)
    print ''
    printBillData(productionCost)
    print ''
    printBillData(testbedCost)
    
    body = generateHtmlReport(totalCost, productionCost, testbedCost, linked_account1, linked_account2, currency)
    
    removeBillingZip(os.path.join(billingDir, detailedBillingFile))
    
    return body

def main(month=None):
    
    if platform.system() == 'Windows':
        configPath = 'billProfile.cfg'
    elif platform.system() == 'Linux':
        configPath = '/opt/dailyBillReport/billProfile.cfg'
        
    config = ConfigParser.ConfigParser()
    config.read([str(configPath)])
    
    month = month or datetime.now().strftime('%Y-%m')
    
    for key,value in config.items('accounts'):
        print '\nprofile: %s' % key

        valDict = ast.literal_eval(value)

        account_number      = valDict['account_number']
        linked_account_id1  = valDict['linked_account_id1']
        linked_account_id2  = valDict['linked_account_id2']
        region              = valDict['region']
        s3bucketname        = valDict['s3bucketname']
        currency            = valDict['currency']

        credentials_profile = None        

        subject = key.title() + ' ' + config.get('mailInfo', 'subject') 
    
        detailedBillingFile = '%s-aws-billing-csv-%s.csv' % (account_number, month)
        
        retVal = retrieve_fd(region, credentials_profile, s3bucketname, detailedBillingFile, billingDir)
        
        if retVal == True:
            body = generateReport(detailedBillingFile, linked_account_id1, linked_account_id2, currency)

            send_mail_by_ses(config.get('mailInfo', 'from'),
                             config.get('mailInfo', 'to'),
                             ast.literal_eval(config.get('mailInfo', 'cc')),
                             config.get('mailInfo', 'from'),
                             subject, 
                             body)

if __name__ == '__main__':
    main()
