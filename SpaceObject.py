class SpaceObject:
    def __init__(self, weight, absorbed = False):
        self.weight = weight
        self.absorbed = absorbed

    def weight(self):
        return self.weight()

    def mark_absorbed(self):
        self.absorbed  = True


class Planet(SpaceObject):
    def __init__(self, weight):
        super().__init__(weight)

    def __add__(self, other):
        return Planet(self.weight + other.weight)

s_obj = SpaceObject(10)
print(s_obj.weight)   # 10
print(s_obj.absorbed) # False
s_obj.mark_absorbed()
print(s_obj.absorbed) # True
res = Planet(100) + Planet(200)
print(res.weight)     # 300