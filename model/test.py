from strategy import *
import pickle
import pymongo
import pandas as pd
from math import log
from time import time
import sys
from scipy.stats import linregress

with open('data', 'r') as file2:
    model = pickle.load(file2)

fit_and_trade(model,3000,0.00005)
