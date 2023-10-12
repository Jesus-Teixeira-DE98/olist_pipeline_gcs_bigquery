import pandas as pd


def clean_data(data):
    data['review_comment_title'].fillna("", inplace=True)
    data['review_comment_message'].fillna("", inplace=True)
    data['review_creation_date'] = pd.to_datetime(data['review_creation_date'], format="%Y-%m-%d %H:%M:%S")
    data['review_answer_timestamp'] = pd.to_datetime(data['review_answer_timestamp'], format="%Y-%m-%d %H:%M:%S")
    data['review_id'].drop_duplicates(inplace=True)
    data.to_csv('order_review_files/olist_order_reviews_dataset_cleaned.csv', sep=';')
    return None


if __name__ == '__main__':
    data = pd.read_csv('order_review_files/olist_order_reviews_dataset.csv')
    a = clean_data(data)