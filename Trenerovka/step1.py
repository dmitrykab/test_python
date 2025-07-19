'''class DataBase:
    pk = 1 
    title = "Классы и объекты"
    author = "Сергей Балакирев"
    views = 14356
    comments = 12 


class Goods:
    title = "Мороженое"
    weight = 154
    tp = "Еда"
    price = 1024

Goods.price = 2048
Goods.inflation = 100
#print(Goods.__dict__)

class Car:
    pass

setattr(Car, "model", "Тойота")
setattr(Car, "color", "Розовый")
setattr(Car, "number", "П111УУ77")
print(Car.__dict__['color'])

class Notes:
    uid = 1005435
    title = "Шутка"
    author = "И.С. Бах"
    pages = 2


at = getattr(Notes,"author")
print(at)

class Dictionary:
    rus = "Питон"
    eng = "Python"

print(getattr(Dictionary,'rus_word',False))

class TravelBlog:
    total_blogs = 0

tb1 = TravelBlog()
tb1.name = 'Франция'
tb1.days = 6
TravelBlog.total_blogs +=1
tb2 = TravelBlog()
tb2.name = 'Италия'
tb2.days = 5
TravelBlog.total_blogs +=1
'''
class Figure:
    type_fig = 'ellipse'
    color = 'red'

fig1 = Figure()
fig1.start_pt = (10, 5)
fig1.end_pt = (100, 20)
fig1.color = 'blue'
print(Figure.__dict__,fig1.__dict__, sep="\n")
delattr(fig1,'color')

print(fig1.__dict__.keys())
