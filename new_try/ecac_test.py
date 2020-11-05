import csv
import pandas as pd
import sklearn as sk
from datetime import date as dt
from dateutil.relativedelta import relativedelta as rd
from sklearn.ensemble import RandomForestClassifier

print(pd.__version__)
print(sk.__version__)

def dateDelta(date, deltaY, deltaM, deltaD, positive):
    num = str(date)
    dy = int('19'+num[0:2])
    dm = int(num[2:4])
    dd = int(num[4:6])
    d = dt(dy,dm,dd)
    delta = rd(years = deltaY, months = deltaM, days = deltaD)
    #print(f'{dy}/{dm}/{dd}')
    if positive:
        return int((d+delta).strftime('%y%m%d'))
    else:
        return int((d-delta).strftime('%y%m%d'))

def yearDifference(date1, date2):
    num1 = int(date1[0:2])
    num2 = int(date2[0:2])
    return num2-num1

csv_folder = '/home/up201306485/Documents/lastchancecac/bankingdata/'



loan_data = pd.read_csv(csv_folder + 'loan_test' + '.csv', delimiter=';')
account_data = pd.read_csv(csv_folder + 'account' + '.csv', delimiter=';') #Prepared
card_data = pd.read_csv(csv_folder + 'card_test' + '.csv', delimiter=';') #Prepared
client_data = pd.read_csv(csv_folder + 'client' + '.csv', delimiter=';') #Prepared
disp_data = pd.read_csv(csv_folder + 'disp' + '.csv', delimiter=';') #Prepared
district_data = pd.read_csv(csv_folder + 'district' + '.csv', delimiter=';') #Prepared
trans_data = pd.read_csv(csv_folder + 'trans_test' + '.csv', delimiter=';') #Prepared

unique = pd.read_csv(csv_folder + 'unique_mixed' + '.csv', delimiter=';')

#print(client_data.head())

gender = []
year = []
month = []
day = []

#Prepare Client Data
for client in client_data['birth_number']:
    num = str(client)
    y = '19'+num[0]+num[1]
    d = num[4]+num[5]
    g = (1 if int(num[2])>=5 else 0)
    m = (str((int(num[2])-5)) if int(num[2])>=5 else num[2])+num[3]
    gender.append(g)
    year.append(int(y))
    month.append(int(m))
    day.append(int(d))

client_data = client_data.drop(columns=['birth_number'])
client_data['gender'] = gender
client_data['birth_year'] = year
client_data['birth_month'] = month
client_data['birth_day'] = day

#print(client_data.head())

#Prepare Account Data
#print(account_data.head())

frequency = account_data.frequency.unique().tolist()

print(account_data.frequency.unique())

frequencies = []

for freq in account_data.frequency:
    frequencies.append(frequency.index(freq))

account_data.frequency = frequencies
account_data.rename(columns = {'date':'account_date'}, inplace=True)

#print(account_data.head())
year = []
month = []
day = []
for date in account_data.account_date:
    num = str(date)
    y = '19'+num[0]+num[1]
    d = num[4]+num[5]
    m = num[2]+num[3]
    year.append(int(y))
    month.append(int(m))
    day.append(int(d))

account_data =account_data.drop(columns = ['account_date'])
account_data['account_year'] = year
account_data['account_month'] = month
account_data['account_day'] = day

#print(account_data.head())

#Prepare Card Data
#print(card_data.head())
type = card_data.type.unique().tolist()

print(card_data.type.unique())

types = []
for typ in card_data.type:
    types.append(type.index(typ))

card_data.type = types

year = []
month = []
day = []
for issue in card_data.issued:
    num = str(issue)
    y = '19'+num[0]+num[1]
    d = num[4]+num[5]
    m = num[2]+num[3]
    year.append(int(y))
    month.append(int(m))
    day.append(int(d))

card_data = card_data.drop(columns=['issued'])
card_data['issued_year'] = year
card_data['issued_month'] = month
card_data['issued_day'] = day

#print(card_data.head())

#Prepare Disponent Data
#print(disp_data.head())
type = disp_data.type.unique().tolist()
print(disp_data.type.unique())

types = []
for typ in disp_data.type:
    types.append(type.index(typ))

disp_data.type = types
disp_data.rename(columns = {'type':'disp_type'}, inplace=True)
disp_data = disp_data[disp_data['disp_type']==0]

#print(disp_data.head())

#Prepare District Data
#print(district_data.head())

region = district_data.region.unique().tolist()
print(district_data.region.unique())

regions = []
for reg in district_data.region:
    regions.append(region.index(reg))

district_data.region = regions
#district_data.rename(columns = {'type':'disp_type'}, inplace=True)
#print(district_data.head())

dist_to_reg = district_data[["code ", "region"]]
dist_to_reg = dist_to_reg.rename(columns = {"code ":"district_id"})

district_data = district_data.drop(columns = ["code ", "name "])

#print(dist_to_reg.head())

district_data = district_data.rename(columns = {"no. of inhabitants":"inhabitants","no. of municipalities with inhabitants < 499 ":"less499","no. of municipalities with inhabitants 500-1999":"less1999","no. of municipalities with inhabitants 2000-9999 ":"less9999","no. of municipalities with inhabitants >10000 ":"more10000","no. of cities ":"cities","ratio of urban inhabitants ":"ratioUrb","average salary ":"salary","unemploymant rate '95 ":"unemploymant95","unemploymant rate '96 ":"unemploymant96","no. of enterpreneurs per 1000 inhabitants ":"enterpreneurs", "no. of commited crimes '95 ":"crime95","no. of commited crimes '96 ":"crime96"})

#print(district_data.describe())
#print(district_data.dtypes)
district_data['unemploymant95'] = pd.to_numeric(district_data['unemploymant95'],errors='coerce')
district_data['crime95'] = pd.to_numeric(district_data['crime95'],errors='coerce')
#print(district_data.dtypes)

#print(district_data.head())

aggregation_functions = {"inhabitants":'mean',"less499":'mean',"less1999":'mean',"less9999":'mean',"more10000":'mean',"cities":'mean',"ratioUrb":'mean',"salary":'mean',"unemploymant95":'mean',"unemploymant96":'mean',"enterpreneurs":'mean', "crime95":'mean',"crime96":'mean'}
district_new = district_data.groupby(district_data["region"]).aggregate(aggregation_functions)

#print(f'{district_data["name "].unique()} {len(district_data["name "].unique())}')
#print(district_new.head())
#print(district_data.describe())
#print(district_new.describe())

# TODO: switch in other relations from district to region using the dist_to_reg table. Change in: account, client

#account_data['district_id'] = account_data['district_id'].map({})
new_dist_id = []
for dis_id in account_data['district_id']:
    aux = dist_to_reg[dist_to_reg['district_id']==dis_id]
    #print(aux.index[0])
    new_dist_id.append(dist_to_reg[dist_to_reg['district_id']==dis_id]['region'][aux.index[0]])
#print(account_data.head())
account_data['district_id'] = new_dist_id
account_data.rename(columns = {"district_id":"region"}, inplace=True)
#print(account_data.head())
#print('\n\n\n\n')
#print(loan_data[loan_data['account_id']==1787])
#print(trans_data[trans_data['account_id']==1787])

#print(client_data.head())
other_dist_id = []
for dis_id in client_data['district_id']:
    aux = dist_to_reg[dist_to_reg['district_id']==dis_id]
    other_dist_id.append(dist_to_reg[dist_to_reg['district_id']==dis_id]['region'][aux.index[0]])

client_data['district_id'] = other_dist_id
client_data.rename(columns = {"district_id":"region"}, inplace=True)
#print(client_data.head())

i = 0

print(trans_data.type.unique())
credits = trans_data[trans_data['type'] == 'credit']
withdrawals = trans_data[trans_data['type'] != 'credit']

#print(f'{credits.head()}\n\n\n{withdrawals.head()}')
account_ids = []
balance_m1_min = []
balance_m1_max = []
balance_m1_mean = []
balance_m1_std = []
num_trans_m1 = []
balance_m3_min = []
balance_m3_max = []
balance_m3_mean = []
balance_m3_std = []
num_trans_m3 = []
balance_m6_min = []
balance_m6_max = []
balance_m6_mean = []
balance_m6_std = []
num_trans_m6 = []
balance_y1_min = []
balance_y1_max = []
balance_y1_mean = []
balance_y1_std = []
num_trans_y1 = []
balance_min = []
balance_max = []
balance_mean = []
balance_std = []
num_trans = []

credit_m1_min = []
credit_m1_max = []
credit_m1_mean = []
credit_m1_std = []
credit_m1_sum = []
credit_m3_min = []
credit_m3_max = []
credit_m3_mean = []
credit_m3_std = []
credit_m3_sum = []
credit_m6_min = []
credit_m6_max = []
credit_m6_mean = []
credit_m6_std = []
credit_m6_sum = []
credit_y1_min = []
credit_y1_max = []
credit_y1_mean = []
credit_y1_std = []
credit_y1_sum = []
credit_min = []
credit_max = []
credit_mean = []
credit_std = []
credit_sum = []

withdrawal_m1_min = []
withdrawal_m1_max = []
withdrawal_m1_mean = []
withdrawal_m1_std = []
withdrawal_m1_sum = []
withdrawal_m3_min = []
withdrawal_m3_max = []
withdrawal_m3_mean = []
withdrawal_m3_std = []
withdrawal_m3_sum = []
withdrawal_m6_min = []
withdrawal_m6_max = []
withdrawal_m6_mean = []
withdrawal_m6_std = []
withdrawal_m6_sum = []
withdrawal_y1_min = []
withdrawal_y1_max = []
withdrawal_y1_mean = []
withdrawal_y1_std = []
withdrawal_y1_sum = []
withdrawal_min = []
withdrawal_max = []
withdrawal_mean = []
withdrawal_std = []
withdrawal_sum = []

recent_balance = []

for account in loan_data['account_id']:
    date = loan_data[loan_data['account_id']==account]['date'][i]
    m1 = dateDelta(date, 0, 1, 0, False)
    m3 = dateDelta(date, 0, 3, 0, False)
    m6 = dateDelta(date, 0, 6, 0, False)
    y1 = dateDelta(date, 1, 0, 0, False)

    balance = trans_data[trans_data['account_id']==account]

    last_balance = balance.loc[balance['date'].idxmax()]['balance']

    recent_balance.append(last_balance)

    account_credits = credits[credits['account_id']==account]
    account_withdrawals = withdrawals[withdrawals['account_id']==account]

    balance_m1 = balance[(balance['date'] > m1) & (balance['date'] <= date)].balance
    credits_m1 = account_credits[(account_credits['date'] > m1) & (account_credits['date']<= date)].amount
    withdrawals_m1 = account_withdrawals[(account_withdrawals['date'] > m1) & (account_withdrawals['date']<= date)].amount

    balance_m3 = balance[(balance['date'] > m3) & (balance['date'] <= date)].balance
    credits_m3 = account_credits[(account_credits['date'] > m3) & (account_credits['date']<= date)].amount
    withdrawals_m3 = account_withdrawals[(account_withdrawals['date'] > m3) & (account_withdrawals['date']<= date)].amount

    balance_m6 = balance[(balance['date'] > m6) & (balance['date'] <= date)].balance
    credits_m6 = account_credits[(account_credits['date'] > m6) & (account_credits['date']<= date)].amount
    withdrawals_m6 = account_withdrawals[(account_withdrawals['date'] > m6) & (account_withdrawals['date']<= date)].amount

    balance_y1 = balance[(balance['date'] > y1) & (balance['date'] <= date)].balance
    credits_y1 = account_credits[(account_credits['date'] > y1) & (account_credits['date']<= date)].amount
    withdrawals_y1 = account_withdrawals[(account_withdrawals['date'] > y1) & (account_withdrawals['date']<= date)].amount

    #print('account')
    account_ids.append(account)

    #print('balance')
    #print(balance_m1.min())
    #print(balance_m1.head())
    balance_m1_min.append(balance_m1.min())
    balance_m1_max.append(balance_m1.max())
    balance_m1_mean.append(balance_m1.mean())
    balance_m1_std.append(balance_m1.std())
    num_trans_m1.append(balance_m1.count())
    balance_m3_min.append(balance_m3.min())
    balance_m3_max.append(balance_m3.max())
    balance_m3_mean.append(balance_m3.mean())
    balance_m3_std.append(balance_m3.std())
    num_trans_m3.append(balance_m3.count())
    balance_m6_min.append(balance_m6.min())
    balance_m6_max.append(balance_m6.max())
    balance_m6_mean.append(balance_m6.mean())
    balance_m6_std.append(balance_m6.std())
    num_trans_m6.append(balance_m6.count())
    balance_y1_min.append(balance_y1.min())
    balance_y1_max.append(balance_y1.max())
    balance_y1_mean.append(balance_y1.mean())
    balance_y1_std.append(balance_y1.std())
    num_trans_y1.append(balance_y1.count())
    balance_min.append(balance.balance.min())
    balance_max.append(balance.balance.max())
    balance_mean.append(balance.balance.mean())
    balance_std.append(balance.balance.std())
    num_trans.append(balance.balance.count())

    #print('credit')
    credit_m1_min.append(credits_m1.min())
    credit_m1_max.append(credits_m1.max())
    credit_m1_mean.append(credits_m1.mean())
    credit_m1_std.append(credits_m1.std())
    credit_m1_sum.append(credits_m1.sum())
    credit_m3_min.append(credits_m3.min())
    credit_m3_max.append(credits_m3.max())
    credit_m3_mean.append(credits_m3.mean())
    credit_m3_std.append(credits_m3.std())
    credit_m3_sum.append(credits_m3.sum())
    credit_m6_min.append(credits_m6.min())
    credit_m6_max.append(credits_m6.max())
    credit_m6_mean.append(credits_m6.mean())
    credit_m6_std.append(credits_m6.std())
    credit_m6_sum.append(credits_m6.sum())
    credit_y1_min.append(credits_y1.min())
    credit_y1_max.append(credits_y1.max())
    credit_y1_mean.append(credits_y1.mean())
    credit_y1_std.append(credits_y1.std())
    credit_y1_sum.append(credits_y1.sum())
    credit_min.append(account_credits.amount.min())
    credit_max.append(account_credits.amount.max())
    credit_mean.append(account_credits.amount.mean())
    credit_std.append(account_credits.amount.std())
    credit_sum.append(account_credits.amount.sum())

    #print('withdrawal')
    withdrawal_m1_min.append(withdrawals_m1.min())
    withdrawal_m1_max.append(withdrawals_m1.max())
    withdrawal_m1_mean.append(withdrawals_m1.mean())
    withdrawal_m1_std.append(withdrawals_m1.std())
    withdrawal_m1_sum.append(withdrawals_m1.sum())
    #print('withdrawal_m1')
    withdrawal_m3_min.append(withdrawals_m3.min())
    withdrawal_m3_max.append(withdrawals_m3.max())
    withdrawal_m3_mean.append(withdrawals_m3.mean())
    withdrawal_m3_std.append(withdrawals_m3.std())
    withdrawal_m3_sum.append(withdrawals_m3.sum())
    #print('withdrawal_m3')
    withdrawal_m6_min.append(withdrawals_m6.min())
    withdrawal_m6_max.append(withdrawals_m6.max())
    withdrawal_m6_mean.append(withdrawals_m6.mean())
    withdrawal_m6_std.append(withdrawals_m6.std())
    withdrawal_m6_sum.append(withdrawals_m6.sum())
    #print('withdrawal_m6')
    withdrawal_y1_min.append(withdrawals_y1.min())
    withdrawal_y1_max.append(withdrawals_y1.max())
    withdrawal_y1_mean.append(withdrawals_y1.mean())
    withdrawal_y1_std.append(withdrawals_y1.std())
    withdrawal_y1_sum.append(withdrawals_y1.sum())
    #print('withdrawal_y1')
    withdrawal_min.append(account_withdrawals.amount.min())
    withdrawal_max.append(account_withdrawals.amount.max())
    withdrawal_mean.append(account_withdrawals.amount.mean())
    withdrawal_std.append(account_withdrawals.amount.std())
    withdrawal_sum.append(account_withdrawals.amount.sum())

    #print(account)

    #if account == 1787:
    #    print(account_withdrawals.amount.min())

    #print(credits_m1.describe())
    i += 1
    '''for trans in account_credits.date:
        #print(f'{trans} - {date}')
        if trans > date:
            print(f'{loan_data[loan_data["account_id"]==account]}\n---\n{trans}')'''

new_trans = pd.DataFrame(data={'account_id':account_ids, 'last_balance': recent_balance,'balance_m1_min':balance_m1_min,'balance_m1_max':balance_m1_max,'balance_m1_mean':balance_m1_mean,'balance_m1_std':balance_m1_std, 'num_trans_m1': num_trans_m1,'balance_m3_min':balance_m3_min,'balance_m3_max':balance_m3_max,'balance_m3_mean':balance_m3_mean,'balance_m3_std':balance_m3_std, 'num_trans_m3': num_trans_m3,'balance_m6_min':balance_m6_min,'balance_m6_max':balance_m6_max,'balance_m6_mean':balance_m6_mean,'balance_m6_std':balance_m6_std, 'num_trans_m6': num_trans_m6,'balance_y1_min':balance_y1_min,'balance_y1_max':balance_y1_max,'balance_y1_mean':balance_y1_mean,'balance_y1_std':balance_y1_std, 'num_trans_y1': num_trans_y1, 'balance_all_min':balance_min, 'balance_all_max': balance_max, 'balance_all_mean': balance_mean, 'balance_all_std': balance_std, 'num_trans': num_trans,'credit_m1_min':credit_m1_min,'credit_m1_max':credit_m1_max,'credit_m1_mean':credit_m1_mean,'credit_m1_std':credit_m1_std, 'credit_m1_sum': credit_m1_sum,'credit_m3_min':credit_m3_min,'credit_m3_max':credit_m3_max,'credit_m3_mean':credit_m3_mean,'credit_m3_std':credit_m3_std, 'credit_m3_sum': credit_m3_sum,'credit_m6_min':credit_m6_min,'credit_m6_max':credit_m6_max,'credit_m6_mean':credit_m6_mean,'credit_m6_std':credit_m6_std, 'credit_m6_sum': credit_m6_sum,'credit_y1_min':credit_y1_min,'credit_y1_max':credit_y1_max,'credit_y1_mean':credit_y1_mean,'credit_y1_std':credit_y1_std, 'credit_y1_sum': credit_y1_sum, 'credit_all_min':credit_min, 'credit_all_max': credit_max, 'credit_all_mean': credit_mean, 'credit_all_std': credit_std, 'credit_all_sum': credit_sum,'withdrawal_m1_min':withdrawal_m1_min,'withdrawal_m1_max':withdrawal_m1_max,'withdrawal_m1_mean':withdrawal_m1_mean,'withdrawal_m1_std':withdrawal_m1_std, 'withdrawal_m1_sum': withdrawal_m1_sum,'withdrawal_m3_min':withdrawal_m3_min,'withdrawal_m3_max':withdrawal_m3_max,'withdrawal_m3_mean':withdrawal_m3_mean,'withdrawal_m3_std':withdrawal_m3_std, 'withdrawal_m3_sum': withdrawal_m3_sum,'withdrawal_m6_min':withdrawal_m6_min,'withdrawal_m6_max':withdrawal_m6_max,'withdrawal_m6_mean':withdrawal_m6_mean,'withdrawal_m6_std':withdrawal_m6_std, 'withdrawal_m6_sum': withdrawal_m6_sum,'withdrawal_y1_min':withdrawal_y1_min,'withdrawal_y1_max':withdrawal_y1_max,'withdrawal_y1_mean':withdrawal_y1_mean,'withdrawal_y1_std':withdrawal_y1_std, 'withdrawal_y1_sum': withdrawal_y1_sum, 'withdrawal_all_min':withdrawal_min, 'withdrawal_all_max': withdrawal_max, 'withdrawal_all_mean': withdrawal_mean, 'withdrawal_all_std': withdrawal_std, 'withdrawal_all_sum': withdrawal_sum})

new_trans.fillna(0, inplace=True)

#print(new_trans.describe())
#print(new_trans.columns)
#print(new_trans.head())

#print(new_trans.head()) #Merged
#print(client_data.head()) #Merged
#print(account_data.head()) #Merged
#print(card_data.head()) #Merged
#print(disp_data.head()) #Merged
#print(district_new.head()) #Merged
#print(loan_data.head()) #Merged

#mixed_data = card_data
#mixed_data = pd.merge(disp_data, mixed_data, on=['disp_id'])

#mixed_data = mixed_data.drop(columns=['card_id','disp_id'])

#print(mixed_data.describe())

mixed_data = disp_data

mixed_data = pd.merge(mixed_data, client_data, on=['client_id'])

mixed_data = mixed_data.drop(columns=['client_id','region'])

#print(mixed_data.describe())

mixed_data = pd.merge(account_data, mixed_data, on=['account_id'])

#print(mixed_data[mixed_data['account_id']==576])

#print(mixed_data.describe())

mixed_data = pd.merge(mixed_data, district_new, on=['region'])

#print(mixed_data[mixed_data['account_id']==576])

#print(mixed_data[mixed_data.duplicated(['account_id'])])

#print(mixed_data[mixed_data['account_id']==576])

mixed_data = mixed_data.drop(columns=['region'])

#print(mixed_data.describe())

mixed_data = pd.merge(mixed_data, new_trans, on=['account_id'], how='left')

#print(mixed_data.describe())

mixed_data = pd.merge(loan_data, mixed_data, on=['account_id'], how='left')

print(mixed_data.describe())

print(mixed_data.head()['balance_m1_min'])

ages = []

i = 0

for loan_id in mixed_data['loan_id']:
    birth_year = int(mixed_data[mixed_data['loan_id']==loan_id]['birth_year'])
    aux = str(mixed_data[mixed_data['loan_id']==loan_id]['date'][i])
    loan_year = int('19' + aux[0:2])
    yearDiff = loan_year - birth_year
    ages.append(yearDiff)
    i += 1

mixed_data['age'] = ages

#print(loan_data.describe())

#naRows = mixed_data[mixed_data.isna().any(axis=1)]

#print(naRows.loc[:,naRows.isna().any()])

#duplicateRows = mixed_data[mixed_data.duplicated(['loan_id'])]

#print(duplicateRows.sort_values(by='loan_id'))

#mixed_data = mixed_data.dropna()

#print(mixed_data[mixed_data['loan_id'] == 4996].diff())

#print(mixed_data.describe())
#print(mixed_data)
mixed_data = mixed_data[unique.columns]
#print(mixed_data)

#mixed_data.to_csv(csv_folder + 'mixed_test.csv', index=False, sep=';')

rf = RandomForestClassifier(n_estimators = 1000, random_state = 42)
train_features = unique.loc[:,~unique.columns.isin(["status","loan_id"])]
train_labels = unique['status']

#print(train_features)

rf.fit(train_features, train_labels)
test_features = mixed_data[train_features.columns]

predictions = rf.predict(test_features)

test_ids = mixed_data['loan_id']

output = {'loan_id': test_ids, 'status': predictions}

outData = pd.DataFrame(data=output)

print(outData)
outData.to_csv(csv_folder + 'output_rf.csv', index=False, sep=';')