
def outer_func(target_func):
    def inner_func():
        print('target함수 실행 전 입니다.')
        target_func()
        print('target함수 실행 후 입니다.')
    
    return inner_func

@outer_func
def get_data():
    print('함수를 시작합니다')



# a = outer_func(get_data)
# a()


# get_data()

def regist(name, sex, *args):
    print(type(args))
    country = args[0] if len(args) >= 1 else None
    city = args[1] if len(args) >= 2 else None

def some_func(**kwargs):
    print(type(kwargs))
    print(kwargs)

    name = kwargs.get('name') or ''
    country = kwargs.get('country') or ''
    print(f'name:{name}, country:{country}')

def regist1(name, sex, *args, **kwargs):
    print(name)
    print(sex)
    print(args)
    print(kwargs)
    

regist('hjkim', 'man')
regist('gdhong', 'man', 'korea', 'seoul')

some_func(name='hjkim', country='kr')

regist1('gdhong', 'man', 'korea', 'seoul', phone='010', email='hjkim_sun@naver.com')