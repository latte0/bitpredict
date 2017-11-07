import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import matplotlib.ticker as mtick


def fit_and_trade(data, split,threshold):

    X = data[[u'width', u'mid', u'imbalance2', u'adj_price2', u'imbalance4',
       u'adj_price4', u'imbalance8', u'adj_price8', u't30_count', u't30_av',
       u'agg30', u'trend30', u't60_count', u't60_av', u'agg60', u'trend60',
       u't120_count', u't120_av', u'agg120', u'trend120', u't180_count',
       u't180_av', u'agg180', u'trend180']]

    y = data.mid60
    print(y)

    print(len(X))
    X_train = X.iloc[:split]
    X_test = X.iloc[split:]
    y_train = y.iloc[:split]
    y_test = y.iloc[split:]
    regressor = RandomForestRegressor(n_estimators=200,
                                      min_samples_leaf=450,
                                      random_state=22,
                                      n_jobs=-1)

#    print(len(X_train.values))
#    print(X_train)
    regressor.fit(X_train.values, y_train.values)
    print(X.columns)

    trade(X_test.values, y_test.values, 50, regressor, threshold)


def trade(X, y, index, model, threshold):
    '''
    Backtests a theoretical trading strategy
    '''
    print 'r-squared', model.score(X, y)
    preds = model.predict(X)
    print(preds)
    trades = np.zeros(len(preds))
    count = 0
    active = False
    n, m = 0, 0
    for i, pred in enumerate(preds):
        if active:
            count += 1
            if count == 6:
                count = 0
                active = False
            m = m+1
        elif abs(pred) > threshold:
            active = True
            trades[i] =  np.sign(pred)
            n = n+1

    print(n)
    print(m)
    returns = trades*y*100
#    print(returns)
#    print(len(returns))
    trades_only = returns[trades != 0]
#    print(trades_only)
    mean_return = trades_only.mean()
    accuracy = sum(trades_only > 0)*1./len(trades_only)
    profit = np.cumsum(returns)
#    print(profit)
    plt.figure(dpi=1000)
    fig, ax = plt.subplots()
    index = range(0,len(profit))
    plt.plot(index, profit)
    plt.title('Trading at Every {}% Prediction (No Transaction Costs)'
              .format(threshold*100))
    plt.ylabel('Returns')
    plt.xticks(rotation=45)
    formatter = mtick.FormatStrFormatter('%.0f%%')
    ax.yaxis.set_major_formatter(formatter)
    return_text = 'Average Return: {:.4f} %'.format(mean_return)
    trades_text = 'Total Trades: {:d}'.format(len(trades_only))
    accuracy_text = 'Accuracy: {:.2f} %'.format(accuracy*100)
    plt.text(.05, .85, return_text, transform=ax.transAxes)
    plt.text(.05, .78, trades_text, transform=ax.transAxes)
    plt.text(.05, .71, accuracy_text, transform=ax.transAxes)
    plt.show()
