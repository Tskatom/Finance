class Foo:
    str = "I'm a str"
    
    def bar(self):
        print self.str
    
    bar = classmethod(bar)

Foo.bar()    