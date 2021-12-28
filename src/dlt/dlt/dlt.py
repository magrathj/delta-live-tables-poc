from functools import wraps

class dlt:
    """DLT wrapper to simulate dlt locally for development purposes"""
    
    def table(*outer_args,**outer_kwargs):                            
        def decorator(fn):                                            
            def decorated(*args,**kwargs): 
                delta_live_table = fn.__name__
                print(f"arguments:{args}, key word arguments:{kwargs}")
                print(f"calling table decorator")
                called_fun = fn(*args,**kwargs)
                print(f"creating table called: {delta_live_table}")
                called_fun.createOrReplaceTempView(delta_live_table)
                print(f"created table called: {delta_live_table}")
                return called_fun                        
            return decorated 
            
        return decorator
    
    def expect(*outer_args,**outer_kwargs):                            
        def decorator(fn):                                            
            def decorated(*args,**kwargs):                            
                print(f"arguments:{args}, key word arguments:{kwargs}")
                print(f"calling expect decorator") 
                called_fun = fn(*args,**kwargs)                
                return called_fun                        
            return decorated                                          
        return decorator
    
    def read(table):                            
        return spark.read.table(table)