def test():
    import time

    start_time = time.time()

    for i in range(5):
        time.sleep(0.5)
    
    end_time = time.time() - start_time

    print("함수 동작 시간 : {}".format(end_time))

test()