class Stack:
    st = []

    def in_steck(self, simbol):
        self.st.append(simbol)
    
    def out_steck(self):
        if len(self.st) > 0:
            return self.st.pop()
    
    def lensteck(self):
        return len(self.st)

    @classmethod
    def delsteck(clc):
        clc.st = []

    def __repr__(self):
        return f"{self.st}"


def is_valid_brackets(str):
    stek = Stack()
    rezult = True
    for i in str:
        if i in ('(','['):
            stek.in_steck(i)
        elif i in (')',']'):
            out = stek.out_steck()
            if (i == ')' and out == '(') or (i == ']' and out == '['):
                continue
            else:
                rezult = False
                break
        else:
            continue

    if stek.lensteck() != 0:
        rezult = False
    
    stek.delsteck()
    return rezult


print(is_valid_brackets('2*(3*(z+4)+5*(10+x))')) # True
print(is_valid_brackets('2*3*(z+4)+5*(10+x))')) # False
print(is_valid_brackets('((((((2+x)*y+1)/10+20)*2-1)*2+1)*10-z)')) # True
print(is_valid_brackets('((((((2+x)*y+1)/10+20)*2-1)*2+1)*10-z')) # False
print(is_valid_brackets('[2,3] + [2.5,5]')) # True
print(is_valid_brackets('[2,3] + 2.5,5]')) # False
print(is_valid_brackets('[[23+x]*2+10*(3+z)-1]')) # True
print(is_valid_brackets('[23+x]*2+10*(3+z)-1]')) # False



