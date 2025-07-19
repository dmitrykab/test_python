import pandas as pd  

df = pd.DataFrame({  
    "user_id": [1, 2, 3, 1, 2, 3, 1],  
    "payment": [3500, 3200, 2200, 2600, 1200, 1800, 1300]  
})  

res = df.groupby("user_id", as_index=False).agg(  
    min_payment=("payment", "min"),  
    max_payment=("payment", "max"),  
    mean_payment=("payment", "mean")  
)  

print(res)