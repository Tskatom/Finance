import time

def f(x,y):
    x+y
    time.sleep(1)
    pass

def main():
    for i in range(1,10):
        f(1,2)

if __name__ == "__main__":
    main()