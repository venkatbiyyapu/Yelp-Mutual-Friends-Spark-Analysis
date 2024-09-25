# Yelp & Mutual Friends Analysis Using Apache Spark on Databricks

## Overview

This project consists of two major parts: 
1. **Yelp Dataset Analysis**: Analyzing user reviews, businesses, and ratings from Yelp data using Apache Spark.
2. **Mutual Friends Analysis**: Finding common friends between pairs of users and additional mutual friend-based insights.

### Key Tasks:
1. **Mutual Friends Calculation**: For each friend pair, calculate the number of common friends.
2. **Identifying Friend Pairs Below Median**: Find pairs whose mutual friend count is below the median.
3. **Yelp User and Business Analysis**: Extract insights from Yelp’s dataset, including average user ratings for Pittsburgh businesses, top-rated businesses, top contributing users, and more.

## Datasets

### Mutual Friends Dataset:
1. **mutual.txt**: Contains the user and their friends in an adjacency list format. Each user is listed with their friends.
    ```
    <User> <TAB> <Friends>
    ```
    - Example:
        ```
        Alice    Bob,Sam,Sara,Nancy
        Bob      Alice,Sam,Clara,Nancy
        ```

### Yelp Dataset:
1. **business.csv**: Contains information about local businesses, including business ID, full address, and categories.
    - Columns:
        - `business_id`: Unique identifier for the business.
        - `full_address`: Localized address of the business.
        - `categories`: Localized categories the business belongs to.
   
2. **review.csv**: Contains user reviews and star ratings for businesses.
    - Columns:
        - `review_id`: Unique identifier for the review.
        - `user_id`: User who authored the review.
        - `business_id`: Business being reviewed.
        - `stars`: Star rating (integer 1-5).

3. **user.csv**: Contains user information such as name and URL.
    - Columns:
        - `user_id`: Unique identifier for the user.
        - `name`: Anonymized user name.
        - `url`: User's Yelp profile URL.

---

## Part 1: Mutual Friends Analysis

### Task 1: Finding Common Friends for Friend Pairs

**Problem Statement**: Given the adjacency list in `mutual.txt`, compute the number of mutual friends for each pair of friends. 
**Script**: mutual_friends_analysis.py
**Input**: 
- **mutual.txt**: A list of users and their friends. Each line represents a user and their friends.

**Output**:
- Each line represents a friend pair and the number of common friends between them.
    ```
    <User_A>, <User_B> <TAB> <Mutual/Common Friend Number>
    ```
    - Example:
        ```
        Alice, Bob    2
        ```

**Steps**:
1. Parse the input to extract each user and their list of friends.
2. For each pair of friends, compute their common friends.
3. Output the pairs with their mutual friend count.

### Task 2: Identifying Pairs with Fewer Common Friends than the Median

**Problem Statement**: From the friend pairs and mutual friend count calculated in Task 1, find pairs with a mutual friend count below the median.
**Script**: mutual_friends_below_median.py
**Input**: 
- **mutual.txt** (same as Task 1).

**Output**:
- Pairs whose mutual friend count is lower than the median.
    ```
    <User_A>, <User_B> <TAB> <Mutual/Common Friend Number>
    ```

**Steps**:
1. Compute mutual friend counts for each pair (from Task 1).
2. Find the median of the mutual friend counts.
3. Filter and output pairs whose mutual friend count is below the median.

---

## Part 2: Yelp Dataset Analysis

### Task 3: List Users and Average Ratings for Pittsburgh Businesses

**Problem Statement**: For businesses located in Pittsburgh, list the names of users and their average rating for those businesses.
**Script**: pittsburgh_user_ratings.py
**Input**: 
- **business.csv**: Contains business details and location.
- **review.csv**: Contains review ratings and business-user mappings.
- **user.csv**: Contains user information.

**Output**:
- A list of users who reviewed Pittsburgh businesses, along with their average rating.
    ```
    Username    Rating
    John Snow   4.0
    ```

**Steps**:
1. Filter `business.csv` for businesses in Pittsburgh.
2. Join the Pittsburgh businesses with `review.csv` to get user reviews for those businesses.
3. Join the result with `user.csv` to get user names.
4. Calculate the average rating per user and output the result.

### Task 4: Top 10 Businesses by Average Rating

**Problem Statement**: Find the top 10 businesses by their average rating.
**Script**: pittsburgh_user_ratings.py
**Input**:
- **business.csv**: Contains business details.
- **review.csv**: Contains review ratings.

**Output**:
- The top 10 businesses along with their categories and average ratings.
    ```
    Business ID    Categories    AVG Rating
    xdf12344444444 ['Local Services', 'Carpet Cleaning']    5.0
    ```

**Steps**:
1. Calculate the average rating for each business using `review.csv`.
2. Join with `business.csv` to get business details and categories.
3. Output the top 10 businesses based on average rating.

### Task 5: Top 10 Users by Contribution to Reviews

**Problem Statement**: Identify the top 10 users who contributed the most reviews as a percentage of total reviews.
**Script**: top_users_by_review_contribution.py
**Input**:
- **review.csv**: Contains review details.

**Output**:
- The top 10 users and their contribution percentages.
    ```
    name    contribution
    John Snow    5%
    ```

**Steps**:
1. Count the total number of reviews.
2. Count the number of reviews contributed by each user.
3. Calculate the contribution percentage for each user.
4. Output the top 10 users by contribution percentage.

### Task 6: Unique Reviewers and Lowest Rating per Business Category

**Problem Statement**: For each business category, find the number of unique reviewers and the lowest rating received.
**Script**: category_unique_reviewers.py
**Input**:
- **business.csv**: Contains business details and categories.
- **review.csv**: Contains review ratings.

**Output**:
- A list of business categories, along with the number of unique reviewers and the lowest rating for that category.
    ```
    Category    No. of unique reviewers    Lowest rating
    Cleaning    50    4.2
    ```

**Steps**:
1. Join `business.csv` and `review.csv` on `business_id`.
2. For each business category, calculate the number of unique reviewers and the lowest rating.
3. Output the results for each category.

---

## Running the Project on Databricks

To run this project on Databricks, follow these steps:

1. **Create a Databricks Workspace**: Set up an account and create a Databricks workspace if you haven't already.

2. **Upload Dataset Files**: 
   - Navigate to the Databricks workspace.
   - Click on `Data` in the sidebar.
   - Select `Add Data` and then `Upload File`.
   - Upload the following dataset files to `/FileStore/tables/`:
     - `mutual.txt`
     - `business.csv`
     - `review.csv`
     - `user.csv`

3. **Create a New Notebook**: 
   - In your Databricks workspace, create a new notebook using existing .py files.
   - Select Python as the language.

4. **Load the Data**: Use Spark to load the dataset files from the `/FileStore/tables/` directory. Example code to load the datasets:
   ```python
    # Load required libraries
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = SparkSession.builder.appName("Yelp Analysis").getOrCreate()
    
    # Load data
    business_df = spark.read.csv("/FileStore/tables/business.csv", header=True, inferSchema=True)
    review_df = spark.read.csv("/FileStore/tables/review.csv", header=True, inferSchema=True)
    user_df = spark.read.csv("/FileStore/tables/user.csv", header=True, inferSchema=True)
    mutual_df = spark.read.text("/FileStore/tables/mutual.txt")

   ```

5. **Execute Each Task**: Open each of the provided .py files in a Databricks notebook or a separate editor. Run the scripts in the order you prefer. Each script processes the input data, performs necessary joins, calculations, and outputs the results as specified.
