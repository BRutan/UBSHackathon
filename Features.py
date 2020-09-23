# 1) Beat the S&P 500:
from abc import ABC, abstractmethod
from pandas import DataFrame

# Abstract base class is Feature. Derived
# must override Extract(self, X, **kwargs) where 
# X is the dictionary version of the dataframe (for
# faster computation) and **kwargs are optional arguments
# used to extract data.
class Feature(ABC):
    @abstractmethod
    def Extract(self, X, **kwargs):
        pass

class TimeFeatures(Feature):
    def Extract(self, X, **kwargs):
        pass

class ShockFeatures(Feature):
    def Extract(self, X, **kwargs):
        pass


# 2) Complex Option Pricing:
# TBA

def returns(X):
    """
    * Compute returns.
    """
    pass

def getfeatures(X, feats, **kwargs):
    """
    * Return dataset with additional features
    appended for each time index.
    """    
    X = X.to_dict()
    for feat in feats:
        X = feat.Extract(X, kwargs)
    return DataFrame(X)

if __name__ == '__main__':
    features = []
    kwargs = {}
