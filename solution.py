import csv
import argparse
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import pandas as pd
import pandasql as ps  

@dataclass_json
@dataclass
class Transaction:
    id: str
    user_id: str
    product_id: str
    price_usd: float

@dataclass_json
@dataclass
class TransactionList:
    transactions: list[Transaction]


def main(data_path, report_path):
    with open(data_path, 'r') as f:
        t_list: TransactionList = TransactionList.schema().loads(f.read())
        #print(len(t_list.transactions))
        df = pd.DataFrame(t_list.transactions)
        #print(df)
        new_df = df.drop_duplicates(subset=['id'])
        #print(new_df.drop)
        #df.to_csv('report_all.csv', index=False)  
        #print(new_df)
        query = '''
                select user_id, product_id as best_product_id
                from (
                select distinct user_id, product_id,
                count(id) over(partition by user_id, product_id) as count_id              
                   from new_df)
                group by user_id
                having count_id = max(count_id)
                   '''

        new_t = ps.sqldf(query, locals())
        #print(new_t)

        query = '''select user_id,
                round(min(price_usd),2) as min_price_usd,
                round(max(price_usd),2) as max_price_usd,
                round(avg(price_usd),2) as avg_price_usd                
                   from new_df 
                   group by user_id'''
        new_t1 = ps.sqldf(query, locals())
        #print(new_t1)


        res_df = new_t1.merge(new_t, on=["user_id"], how="left")  
        #print(res_df)  
        res_df.to_csv(report_path, index=False)  

   


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='transactions.json', help='Please set datasets path.')
    parser.add_argument('--report_path', type=str, default='report.csv', help='Please set report path.')
    args = parser.parse_args()
    data_path = args.data_path
    report_path = args.report_path
    main(data_path, report_path)

