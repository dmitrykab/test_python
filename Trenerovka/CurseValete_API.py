import requests

def get_currency_rate(currency_code):
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    response = requests.get(url)

    if response.status_code != 200:
        print("Error fetching currency data.")
        return

    data = response.json()
    currency = data["Valute"].get(currency_code)
    date_cusr = data.get("Date")

    if currency:
        print(f"Курс валют на {date_cusr[0:10]}")
        print(f"Currency: {currency['Name']}")
        print(f"Current rate: {currency['Value']} RUB")
        print(f"Previous rate: {currency['Previous']} RUB")
    else:
        print("Currency not found.")

# Пример использования
get_currency_rate("USD")
get_currency_rate("EUR")