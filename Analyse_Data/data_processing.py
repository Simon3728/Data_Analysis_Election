"""
This file contains functions for loading, normalizing, and splitting data,
as well as functions for performing KNN, linear, and polynomial regression.
"""

import pandas as pd
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.metrics import r2_score, mean_squared_error
import numpy as np
from create_final_data import create_final_data
from sklearn.pipeline import Pipeline

def load_and_normalize_data(years, features, response):
    """
    Load and normalize the data.
    """
    df = create_final_data(years)
    X = df[features]
    y = df[response]
    scaler_X = StandardScaler()
    scaler_y = StandardScaler()
    X = scaler_X.fit_transform(X)
    y = scaler_y.fit_transform(y.values.reshape(-1, 1)).flatten()
    return X, y

def split_data(X, y, test_size=0.2, random_state=42):
    """
    Split the data into training and test sets.
    """
    return train_test_split(X, y, test_size=test_size, random_state=random_state)

def perform_knn_cv(X_train, y_train, k_range):
    """
    Perform KNN regression with cross-validation to select the best k.
    """
    param_grid = {'n_neighbors': k_range}
    knn = KNeighborsRegressor()
    grid_search = GridSearchCV(knn, param_grid, cv=5, scoring='r2')
    grid_search.fit(X_train, y_train)
    best_k = grid_search.best_params_['n_neighbors']
    best_r2 = grid_search.best_score_
    return best_k, best_r2, grid_search.best_estimator_

def perform_linear_regression_cv(X_train, y_train):
    """
    Perform linear regression with cross-validation to evaluate the model.
    """
    lr = LinearRegression()
    scores = cross_val_score(lr, X_train, y_train, cv=5, scoring='r2')
    mean_r2 = scores.mean()
    lr.fit(X_train, y_train)
    return mean_r2, lr

def evaluate_metrics(model, X_train, X_test, y_train, y_test):
    """
    Evaluate model performance using R^2 and MSE metrics.
    """
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    metrics = {
        'R2_train': r2_score(y_train, y_pred_train),
        'R2_test': r2_score(y_test, y_pred_test),
        'MSE_test': mean_squared_error(y_test, y_pred_test)
    }
    return metrics, y_pred_test

def greedy_feature_selection_knn(X, y, features, k_range):
    """
    Perform greedy feature selection for KNN.
    """
    selected_features = []
    remaining_features = features.copy()
    best_r2 = -np.inf
    best_model = None

    while remaining_features:
        best_new_feature = None
        for feature in remaining_features:
            current_features = selected_features + [feature]
            X_current = X[:, [features.index(f) for f in current_features]]
            _, current_r2, model = perform_knn_cv(X_current, y, k_range)
            if current_r2 > best_r2:
                best_r2 = current_r2
                best_new_feature = feature
                best_model = model
        if best_new_feature:
            selected_features.append(best_new_feature)
            remaining_features.remove(best_new_feature)
        else:
            break
    return selected_features, best_model

def greedy_feature_selection_linear(X, y, features):
    """
    Perform greedy feature selection for linear regression with cross-validation.
    """
    selected_features = []
    remaining_features = features.copy()
    best_r2 = -np.inf
    best_model = None

    while remaining_features:
        best_new_feature = None
        for feature in remaining_features:
            current_features = selected_features + [feature]
            X_current = X[:, [features.index(f) for f in current_features]]
            current_r2, model = perform_linear_regression_cv(X_current, y)
            if current_r2 > best_r2:
                best_r2 = current_r2
                best_new_feature = feature
                best_model = model
        if best_new_feature:
            selected_features.append(best_new_feature)
            remaining_features.remove(best_new_feature)
        else:
            break
    return selected_features, best_r2, best_model

def train_polynomial_regression_single_feature(X, y, feature_index, degree):
    """
    Train a polynomial regression model on a single feature.
    """
    # Extract the single feature
    X_feature = X[:, feature_index].reshape(-1, 1)
    
    # Create and fit polynomial features
    poly_features = PolynomialFeatures(degree=degree)
    X_poly = poly_features.fit_transform(X_feature)
    
    # Train linear regression model on the polynomial features
    model = LinearRegression().fit(X_poly, y)
    
    return model, poly_features 

def grid_search_feature_selection_poly(X, y, features, degrees):
    """
    Perform feature selection for polynomial regression with GridSearchCV.
    """
    selected_features = []
    remaining_features = features.copy()
    best_r2 = -np.inf
    best_model = None
    best_poly_features = None
    best_degree = None

    while remaining_features:
        best_new_feature = None
        for feature in remaining_features:
            current_features = selected_features + [feature]
            X_current = X[:, [features.index(f) for f in current_features]]
            model = Pipeline([
                ('poly', PolynomialFeatures()),
                ('linear', LinearRegression())
            ])
            param_grid = {'poly__degree': degrees}
            grid_search = GridSearchCV(model, param_grid, cv=5, scoring='r2')
            grid_search.fit(X_current, y)
            current_r2 = grid_search.best_score_
            if current_r2 > best_r2:
                best_r2 = current_r2
                best_new_feature = feature
                best_model = grid_search.best_estimator_
                best_poly_features = grid_search.best_estimator_.named_steps['poly']
                best_degree = grid_search.best_params_['poly__degree']
        if best_new_feature:
            selected_features.append(best_new_feature)
            remaining_features.remove(best_new_feature)
        else:
            break
    return selected_features, best_r2, best_model, best_poly_features, best_degree