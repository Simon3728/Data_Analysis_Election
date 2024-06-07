"""
This file contains functions for plotting the results of KNN, linear, and polynomial regression models.
"""

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import r2_score

def plot_knn_results(X_train, X_test, y_train, y_test, y_pred_test, best_k, r2_train, r2_test, features, response_name, feature_index):
    """
    Plot KNN regression results, display only the best predictor in the plot
    """
    plt.figure(figsize=(10, 6))
    
    # Plot training data points
    plt.scatter(X_train[:, feature_index], y_train, color='black', label='Training Data')
    
    # Plot testing data points
    plt.scatter(X_test[:, feature_index], y_test, color='red', label='Test Data')
    
    # Sort the test feature for a smooth line plot
    sorted_idx = np.argsort(X_test[:, feature_index])
    
    # Plot the predicted values (from KNN) against the test feature
    plt.plot(X_test[sorted_idx, feature_index], y_pred_test[sorted_idx], label=f'k={best_k}', color='blue')
    
    plt.xlabel(features[feature_index])
    plt.ylabel(response_name)
    plt.title(f'KNN Regression with k = {best_k} and features: {", ".join(features)}\nTrain R²: {r2_train:.2f}, Test R²: {r2_test:.2f}')
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_linear_regression_results(X_train, X_test, y_train, y_test, model, feature_name, response_name, selected_features, r2_train, r2_test, feature_index):
    """
    Plot linear regression results, display only the best predictor in the plot
    """
    plt.figure(figsize=(10, 6))
    
    # Plot training data points
    plt.scatter(X_train[:, feature_index], y_train, color='black', label='Training Data')
    
    # Plot testing data points
    plt.scatter(X_test[:, feature_index], y_test, color='red', label='Test Data')
    
    # Generate a range of values for the feature of interest
    x_range = np.linspace(X_test[:, feature_index].min(), X_test[:, feature_index].max(), 100).reshape(-1, 1)
    X_plot = np.repeat(np.mean(X_test, axis=0).reshape(1, -1), 100, axis=0)
    X_plot[:, feature_index] = x_range.flatten()
    
    # Predict and plot the linear regression line
    y_plot = model.predict(X_plot)
    plt.plot(x_range, y_plot, label=f'Linear Regression', color='green')
    
    plt.xlabel(feature_name)
    plt.ylabel(response_name)
    
    # Set plot title with more readable formatting
    plt.title(f'Linear Regression with features: {", ".join(selected_features)}\nTrain R²: {r2_train:.2f}, Test R²: {r2_test:.2f}')
    
    plt.legend()
    plt.grid(True)
    plt.show()


def plot_polynomial_regression_results_single_feature(X_train, X_test, y_train, y_test, model, poly_features, degree, feature_name, response, feature_index=0):
    """
    Plot the results of polynomial regression for a single feature.
    """
    # Extract the single feature
    X_train_feature = X_train[:, feature_index].reshape(-1, 1)
    X_test_feature = X_test[:, feature_index].reshape(-1, 1)

    # Transform the feature data using the provided PolynomialFeatures object
    X_train_poly = poly_features.transform(X_train_feature)
    X_test_poly = poly_features.transform(X_test_feature)

    # Predict
    y_train_pred = model.predict(X_train_poly)
    y_test_pred = model.predict(X_test_poly)

    # Calculate R^2 scores
    r2_train = r2_score(y_train, y_train_pred)
    r2_test = r2_score(y_test, y_test_pred)

    # Plot
    plt.figure(figsize=(10, 6))
    plt.scatter(X_train_feature, y_train, color='black', label='Training Data')
    plt.scatter(X_test_feature, y_test, color='red', label='Test Data')
    X_plot = np.linspace(X_train_feature.min(), X_train_feature.max(), 100).reshape(-1, 1)
    X_plot_poly = poly_features.transform(X_plot)
    plt.plot(X_plot, model.predict(X_plot_poly), color='purple', label=f'Polynomial Regression (degree={degree}) (Train $R^2$={r2_train:.2f}, Test $R^2$={r2_test:.2f})')
    plt.xlabel(feature_name)
    plt.ylabel(response)
    plt.title(f'Example Polynomial Regression (Degree {degree}) for {feature_name} vs {response}')
    plt.legend()
    plt.grid(True)
    plt.show()

