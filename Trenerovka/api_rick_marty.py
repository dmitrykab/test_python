import requests

def get_count():
        url = "https://rickandmortyapi.com/api/character"
        response = requests.get(url)
        print(response.json()["results"])


        if response.status_code == 200:
            root = response.json()
            return (root["info"]["count"])
        else: 
            return ("Ошибка api:", response.status_code)


a = get_count()
print(a)