"""
This file contains the main script for performing data analysis using KNN,
linear, and polynomial regression models. It contains the data loading,
model training, feature selection, evaluation, and plotting.
"""

from data_processing import *
from plot_functions import *

def main():
    years = [2000, 2004, 2008, 2012, 2016, 2020]
    features = ['gdp_per_capita', 'college_finishers', 'unemployment_rate', 'urban_population', 'age_18_24_percent', 'age_25_44_percent', 'age_45_64_percent', 'age_65_plus_percent']
    response = 'republican_percent'
    k_range = list(range(7, 80))
    degrees = list(range(1, 6))

    X, y = load_and_normalize_data(years, features, response)
    X_train, X_test, y_train, y_test = split_data(X, y)
    
    ############################# KNN #############################
    selected_features_knn, best_model_knn = greedy_feature_selection_knn(X_train, y_train, features, k_range)

    print(f'Selected features (KNN): {selected_features_knn}')

    X_selected_train_knn = X_train[:, [features.index(f) for f in selected_features_knn]]
    X_selected_test_knn = X_test[:, [features.index(f) for f in selected_features_knn]]

    metrics_knn, y_pred_test_knn = evaluate_metrics(best_model_knn, X_selected_train_knn, X_selected_test_knn, y_train, y_test)
    for metric, value in metrics_knn.items():
        print(f'{metric} (KNN): {value:.4f}')
        
    plot_knn_results(X_selected_train_knn, X_selected_test_knn, y_train, y_test, y_pred_test_knn, best_model_knn.n_neighbors, metrics_knn['R2_train'], metrics_knn['R2_test'], selected_features_knn, response, feature_index=0)
    
    ############################# Linear regression #############################

    selected_features_lr, best_r2_lr, best_model_lr = greedy_feature_selection_linear(X_train, y_train, features)

    print(f'Selected features (Linear Regression): {selected_features_lr}')
    
    X_selected_train_lr = X_train[:, [features.index(f) for f in selected_features_lr]]
    X_selected_test_lr = X_test[:, [features.index(f) for f in selected_features_lr]]
    metrics_lr, _ = evaluate_metrics(best_model_lr, X_selected_train_lr, X_selected_test_lr, y_train, y_test)

    for metric, value in metrics_lr.items():
        print(f'{metric} (Linear Regression): {value:.4f}')

    plot_linear_regression_results(X_selected_train_lr, X_selected_test_lr, y_train, y_test, best_model_lr, selected_features_lr[0], response, selected_features_lr, metrics_lr['R2_train'], metrics_lr['R2_test'], feature_index=0)

    ############################# Polynomial regression #############################

    selected_features_pr, _, best_model_pr, best_poly_features_pr, best_degree_pr = grid_search_feature_selection_poly(X_train, y_train, features, degrees)
    print(f'Selected features (Polynomial Regression): {selected_features_pr}')
    print(f'Best polynomial degree: {best_degree_pr}')  

    X_selected_train_pr = X_train[:, [features.index(f) for f in selected_features_pr]]
    X_selected_test_pr = X_test[:, [features.index(f) for f in selected_features_pr]]
    X_selected_train_pr_poly = best_poly_features_pr.transform(X_selected_train_pr)
    X_selected_test_pr_poly = best_poly_features_pr.transform(X_selected_test_pr)
    metrics_pr, _ = evaluate_metrics(best_model_pr.named_steps['linear'], X_selected_train_pr_poly, X_selected_test_pr_poly, y_train, y_test)

    for metric, value in metrics_pr.items():
        print(f'{metric} (Polynomial Regression): {value:.4f}')
    
    # Plot a Polyfunction on the best feature just to display one example
    first_selected_feature_index = features.index(selected_features_pr[0])
    best_feature_model, best_feature_poly_features = train_polynomial_regression_single_feature(X_train, y_train, first_selected_feature_index, best_degree_pr)
    plot_polynomial_regression_results_single_feature(X_train, X_test, y_train, y_test, best_feature_model, best_feature_poly_features, best_degree_pr, selected_features_pr[0], 'republican_percent', feature_index=first_selected_feature_index)

if __name__ == "__main__":
    main()
