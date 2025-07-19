'''def deposit_result(value, p, m_count):
    while m_count != 1:
        value = deposit_result(value,p,m_count-1)
        m_count = m_count -1
    rez = value*(1+p)
    return rez'''


def deposit_result(value, p, m_count):
    if m_count == 1:
        rez = value*(1+p)
    elif m_count == 0:
        rez = value
    else:
        rez = deposit_result(value,p,m_count-1)*(1+p) 
    return rez

#print(deposit_result(50000, 0.05, 4))#(52500, 55125,57881,25,60775.3125)
print(deposit_result(50000, 0.05, 10)) # 81444.73133887208