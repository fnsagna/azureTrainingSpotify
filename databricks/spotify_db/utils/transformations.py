class reusable:
    
    def dropColumns(self, df, columns):
        return df.drop(*columns)
    
    def dropDuplicates(self,df):
        return df.dropDuplicates()
    
    def dropNulls(self,df):
        return df.dropna()
    
    def dropEmpty_strings(self,df):    
        return df.filter(~(df.user_name == ""))
    