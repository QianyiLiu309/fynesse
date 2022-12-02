# This file contains code for suporting addressing questions in the data

import datetime
import statsmodels.api as sm
from address import (
    get_joined_transactions,
    download_POI_for_feature_list,
    calculate_features,
)
import pandas as pd

"""Address a particular question that arises from the data"""


def poisson_regression_predict(df, design, observed_df):
    """Fit a poisson regression model with lasso regularization using feature `design`.
    Then use the model to predict housing price based on observed features `observed_df`
    :param df: a dataframe containing column `price`
    :param design: a dataframe representing the design matrix
    :param observed_df: a dataframe for observed features
    :return: the predicted housing price as a float
    """
    m_linear_basis = sm.GLM(
        df["price"].to_numpy(), design, family=sm.families.Poisson()
    )
    results_basis = m_linear_basis.fit_regularized(alpha=0.05, L1_wt=1)
    results_basis = m_linear_basis.fit()

    print("Feature weights: ", results_basis.params)

    return results_basis.predict(
        observed_df.drop(["lattitude", "longitude"], axis=1).values
    )[0]


def gaussian_regression_predict(df, design, observed_df):
    """Fit a gaussian regression model using feature `design`.
    Then use the model to predict housing price based on observed features `observed_df`
    :param df: a dataframe containing column `price`
    :param design: a dataframe representing the design matrix
    :param observed_df: a dataframe for observed features
    :return: the predicted housing price as a float
    """
    m_linear_basis = sm.GLM(
        df["price"].to_numpy(), design, family=sm.families.Gaussian()
    )
    results_basis = m_linear_basis.fit()

    print("Feature weights: ", results_basis.params)

    return results_basis.predict(
        observed_df.drop(["lattitude", "longitude"], axis=1).values
    )[0]


def predict_price(
    conn,
    latitude,
    longitude,
    date,
    property_type,
    args,
    model_name="poisson",
    debug_mod=True,
    df_filter=None,
):
    """Retrieve the joined transaction records from database and construct a
    training set of housing prices at the specified location with dynamically
    adjusted bbox size. Fit the specified GLM, then predict the price for the house with
    `property_type` at certain date at the specified location.
    :param conn: a connection to the database
    :param latitude: latitude
    :param longitude: longitude
    :param date: a datetime object
    :param args: a dict representing hyperparameters
    :param model_name: the name of the GLM to be used
    :param debug_mode: a boolean, only print features when set to true
    :param df_filter: a function to filter out validation entry from the training set
    :return: a flaot value representing the predicted housing price
    """
    start_date = date - datetime.timedelta(days=args["time_range"] // 2)
    end_date = date + datetime.timedelta(days=args["time_range"] // 2)
    print(f"Considering time range: {str(start_date)} to {str(end_date)}")

    bbox_size = args["bbox_size_initial"]
    required_sample_size = args["required_sample_size"]
    features = args["features"]

    while True:
        transaction_df = get_joined_transactions(
            conn,
            start_date,
            end_date,
            latitude,
            longitude,
            bbox_size,
            bbox_size,
            property_type,
        )
        if len(transaction_df) < required_sample_size:
            bbox_size *= args["increase_factor"]
            if bbox_size > args["bbox_size_limit"]:
                print(
                    f"The number of samples is {len(transaction_df)}, which is less than the requirement {required_sample_size}. Consider properties of all types. Prediction results may be inaccurate in this case."
                )
                break
        else:
            break

    # Build up the training set from housing of all types
    if len(transaction_df) < required_sample_size:
        transaction_df = get_joined_transactions(
            conn, latitude, longitude, bbox_size, bbox_size, start_date, end_date
        )

    print(f"Retrieved {len(transaction_df)} transactions with bbox size = {bbox_size}")

    if len(transaction_df) == 0:
        return "No samples in the training set"

    # used to filter out validation entry during evaluation
    if df_filter is not None:
        transaction_df = df_filter(transaction_df)

    # bbox for features
    feature_bbox_size = bbox_size * 2
    dist_threshold = bbox_size / 2

    df = transaction_df[["price", "lattitude", "longitude"]].copy()
    pois_map = download_POI_for_feature_list(
        latitude, longitude, bbox_size, bbox_size, features
    )
    df = calculate_features(df, features, dist_threshold, pois_map)
    df["one"] = 10
    if debug_mod:
        print("Features: ", df)

    observed_df = pd.DataFrame({"lattitude": [latitude], "longitude": [longitude]})
    observed_df = calculate_features(observed_df, features, dist_threshold, pois_map)
    observed_df["one"] = 10
    if debug_mod:
        print("Observed features: ", observed_df)

    design = df.drop(["price", "lattitude", "longitude"], axis=1).values

    if model_name == "poisson":
        return poisson_regression_predict(df, design, observed_df)
    elif model_name == "gaussian":
        return gaussian_regression_predict(df, design, observed_df)
    else:
        raise NotImplementedError


def filter_out_validation_data(transaction_df, val_db_id):
    """Filter out the record specified by val_df_id from the training dataset
    :param transaction_df: the training set of price data
    :param val_db_id: the db_id of the validation data point
    :return: a dataframe, with one entry removed from transaction_df
    """
    filtered_df = transaction_df[transaction_df.db_id != val_db_id]
    if len(filtered_df) < len(transaction_df):
        print(f"Successfully removed the entry with db_id: {val_db_id}")
    return filtered_df


def evaluate_model(conn, validation_df, model_name, args):
    """Evaluate the specified model using the known price of a property
    :param conn: a connection to db
    :param validation_df: the validation dataset of property price
    :param model_name: a string representing model name
    :param args: hyperparameters to the model
    :return: a pair of arrays containing real property prices and predicted property prices

    """
    real_price_ls = []
    predicted_price_ls = []
    for _, row in validation_df.iterrows():
        val_db_id = row.db_id
        real_price = row.price
        filter_fn = lambda df: filter_out_validation_data(df, val_db_id)
        predicted_price = predict_price(
            conn,
            row.lattitude,
            row.longitude,
            row.date_of_transfer,
            row.property_type,
            args,
            model_name,
            debug_mod=False,
            df_filter=filter_fn,
        )
        real_price_ls.append(real_price)
        predicted_price_ls.append(predicted_price)
    return real_price_ls, predicted_price_ls
