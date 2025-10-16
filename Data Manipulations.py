"""
Python is a versatile programming language widely used for data analysis, 
machine learning, and automation. These scripts demonstrate real-world 
data processing scenarios.
"""

# 1. Advanced ETL Pipeline with Error Handling
"""
ETL (Extract, Transform, Load): Process of extracting data from sources, 
transforming it to fit business needs, and loading into a destination system.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

class DataETLPipeline:
    """
    A comprehensive ETL pipeline class that handles data extraction,
    transformation, and loading with robust error handling and logging.
    """
    
    def __init__(self):
        """Initialize the pipeline with logger setup."""
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """
        Configure logging to track pipeline execution and errors.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        logging.basicConfig(
            level=logging.INFO,  # Log level: DEBUG, INFO, WARNING, ERROR
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def extract_data(self, file_paths):
        """
        Extract data from multiple file sources.
        
        Args:
            file_paths (dict): Dictionary with keys 'sales', 'customers', 'products'
                              containing file paths
                              
        Returns:
            tuple: Three DataFrames (sales_df, customers_df, products_df)
            
        Raises:
            Exception: If any file cannot be read
        """
        try:
            # Read data from different file formats
            sales_df = pd.read_excel(file_paths['sales'])      # Excel file
            customers_df = pd.read_csv(file_paths['customers']) # CSV file
            products_df = pd.read_json(file_paths['products'])  # JSON file
            
            self.logger.info("Data extraction completed successfully")
            return sales_df, customers_df, products_df
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {str(e)}")
            raise  # Re-raise exception for calling code to handle
    
    def transform_data(self, sales_df, customers_df, products_df):
        """
        Transform and clean the extracted data.
        
        Args:
            sales_df (DataFrame): Raw sales data
            customers_df (DataFrame): Raw customer data  
            products_df (DataFrame): Raw product data
            
        Returns:
            DataFrame: Cleaned and transformed merged dataset
        """
        try:
            # Merge datasets using CustomerID and ProductID as keys
            merged_df = sales_df.merge(
                customers_df, on='CustomerID', how='left'  # LEFT JOIN keeps all sales
            ).merge(
                products_df, on='ProductID', how='left'    # LEFT JOIN keeps all sales
            )
            
            # Apply data cleaning and enrichment steps
            merged_df = self._clean_data(merged_df)
            merged_df = self._enrich_data(merged_df)
            merged_df = self._handle_missing_values(merged_df)
            
            self.logger.info("Data transformation completed successfully")
            return merged_df
            
        except Exception as e:
            self.logger.error(f"Data transformation failed: {str(e)}")
            raise
    
    def _clean_data(self, df):
        """
        Perform data cleaning operations.
        
        Args:
            df (DataFrame): Input dataframe to clean
            
        Returns:
            DataFrame: Cleaned dataframe
        """
        # Remove duplicate rows (keep first occurrence)
        df = df.drop_duplicates()
        
        # Filter out invalid business data
        df = df[df['Quantity'] > 0]   # Only positive quantities
        df = df[df['UnitPrice'] > 0]  # Only positive prices
        
        # Standardize text formatting
        df['CustomerName'] = df['CustomerName'].str.title().str.strip()  # Title case
        df['ProductCategory'] = df['ProductCategory'].str.upper().str.strip()  # Uppercase
        
        return df
    
    def _enrich_data(self, df):
        """
        Add calculated columns and business logic.
        
        Args:
            df (DataFrame): Input dataframe to enrich
            
        Returns:
            DataFrame: Enriched dataframe with new columns
        """
        # Calculate total amount for each line item
        df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
        
        # Convert string dates to datetime objects for time-based analysis
        df['OrderDate'] = pd.to_datetime(df['OrderDate'])
        
        # Extract time-based features
        df['OrderMonth'] = df['OrderDate'].dt.to_period('M')  # Year-Month period
        df['OrderYear'] = df['OrderDate'].dt.year            # Year only
        df['OrderDayOfWeek'] = df['OrderDate'].dt.day_name() # Monday, Tuesday, etc.
        
        # Customer segmentation based on order value
        df['CustomerSegment'] = np.where(
            df['TotalAmount'] > 1000,  # Condition
            'VIP',                      # Value if True
            'Standard'                  # Value if False
        )
        
        return df
    
    def _handle_missing_values(self, df):
        """
        Handle missing values using appropriate strategies.
        
        Args:
            df (DataFrame): Input dataframe with potential missing values
            
        Returns:
            DataFrame: Dataframe with handled missing values
        """
        # Fill missing discounts with 0 (no discount)
        df['Discount'] = df['Discount'].fillna(0)
        
        # Fill missing regions with 'Unknown'
        df['CustomerRegion'] = df['CustomerRegion'].fillna('Unknown')
        
        # For missing prices, use median price of similar products
        df['UnitPrice'] = df.groupby('ProductCategory')['UnitPrice'].transform(
            lambda x: x.fillna(x.median())  # Fill with group median
        )
        
        return df
    
    def load_data(self, df, output_path):
        """
        Load transformed data to destination storage.
        
        Args:
            df (DataFrame): Transformed data to save
            output_path (str): Directory path for output files
        """
        try:
            # Save in multiple formats for different use cases
            df.to_parquet(f"{output_path}/sales_data.parquet", index=False)  # Efficient storage
            df.to_csv(f"{output_path}/sales_data.csv", index=False)          # Human readable
            
            # Create business-ready aggregated views
            self._create_aggregated_views(df, output_path)
            
            self.logger.info("Data loading completed successfully")
            
        except Exception as e:
            self.logger.error(f"Data loading failed: {str(e)}")
            raise
    
    def _create_aggregated_views(self, df, output_path):
        """
        Create summary datasets for business reporting.
        
        Args:
            df (DataFrame): Master dataset to aggregate
            output_path (str): Directory to save summary files
        """
        # Monthly sales summary by category
        monthly_sales = df.groupby(['OrderYear', 'OrderMonth', 'ProductCategory']).agg({
            'TotalAmount': ['sum', 'mean', 'count'],  # Multiple aggregations
            'Quantity': 'sum'
        }).round(2)  # Round to 2 decimal places
        
        # Flatten multi-level column names
        monthly_sales.columns = ['TotalSales', 'AvgOrderValue', 'OrderCount', 'TotalQuantity']
        monthly_sales.reset_index().to_csv(f"{output_path}/monthly_sales_summary.csv", index=False)

# 2. Sales Forecasting with Time Series Analysis
"""
Time Series Analysis: Statistical techniques for analyzing time-based data
to identify patterns, trends, and make future predictions.
"""
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt

def sales_forecasting_analysis(df):
    """
    Perform sales forecasting using machine learning and feature engineering.
    
    Args:
        df (DataFrame): Historical sales data with date and amount columns
        
    Returns:
        tuple: Model, feature importance, test values, and predictions
    """
    
    # Feature engineering for time series data
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df = df.set_index('OrderDate').sort_index()  # Set date as index and sort
    
    # Create time-based features that help model understand patterns
    df['DayOfWeek'] = df.index.dayofweek        # Monday=0, Sunday=6
    df['Month'] = df.index.month                # January=1, December=12
    df['Quarter'] = df.index.quarter            # Q1=1, Q4=4
    df['Year'] = df.index.year                  # 2023, 2024, etc.
    df['IsWeekend'] = (df.index.dayofweek >= 5).astype(int)  # 1 if weekend
    df['IsMonthEnd'] = (df.index.is_month_end).astype(int)   # 1 if month end
    
    # Lag features - previous values that might predict future values
    for lag in [1, 7, 30]:  # 1 day, 1 week, 1 month lags
        df[f'Sales_Lag_{lag}'] = df['TotalAmount'].shift(lag)
    
    # Rolling statistics - moving averages and standard deviations
    df['Sales_Rolling_Mean_7'] = df['TotalAmount'].rolling(window=7).mean()
    df['Sales_Rolling_Std_7'] = df['TotalAmount'].rolling(window=7).std()
    
    # Prepare features for machine learning model
    feature_columns = ['DayOfWeek', 'Month', 'Quarter', 'Year', 'IsWeekend', 'IsMonthEnd',
                      'Sales_Lag_1', 'Sales_Lag_7', 'Sales_Lag_30',
                      'Sales_Rolling_Mean_7', 'Sales_Rolling_Std_7']
    
    # Remove rows with missing values created by lag/rolling features
    modeling_df = df.dropna(subset=feature_columns + ['TotalAmount'])
    
    X = modeling_df[feature_columns]  # Features (input variables)
    y = modeling_df['TotalAmount']    # Target (what we're predicting)
    
    # Split data chronologically (important for time series)
    split_date = modeling_df.index[int(len(modeling_df) * 0.8)]  # 80% for training
    X_train = X[X.index <= split_date]
    X_test = X[X.index > split_date]
    y_train = y[y.index <= split_date]
    y_test = y[y.index > split_date]
    
    # Train Random Forest model
    model = RandomForestRegressor(
        n_estimators=100,  # Number of trees in the forest
        random_state=42     # Seed for reproducible results
    )
    model.fit(X_train, y_train)  # Train the model
    
    # Make predictions on test data
    y_pred = model.predict(X_test)
    
    # Calculate model performance metrics
    mae = mean_absolute_error(y_test, y_pred)  # Average absolute error
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))  # Root mean squared error
    
    print(f"Model Performance:")
    print(f"MAE: {mae:.2f}")   # How wrong predictions are on average
    print(f"RMSE: {rmse:.2f}") # Penalizes larger errors more heavily
    
    # Analyze which features are most important for predictions
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': model.feature_importances_  # How much each feature contributes
    }).sort_values('importance', ascending=False)
    
    return model, feature_importance, y_test, y_pred

# 3. Customer Segmentation with Clustering
"""
Clustering: Unsupervised machine learning technique that groups similar
data points together based on their characteristics.
"""
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import seaborn as sns

def customer_segmentation_analysis(customers_df, sales_df):
    """
    Segment customers into groups using RFM analysis and K-means clustering.
    
    RFM Analysis: Recency, Frequency, Monetary - a method for customer segmentation
    based on their purchasing behavior.
    
    Args:
        customers_df (DataFrame): Customer demographic data
        sales_df (DataFrame): Customer transaction data
        
    Returns:
        tuple: Segmented customers, cluster summary, and clustering model
    """
    
    # Calculate RFM metrics for each customer
    rfm_df = sales_df.groupby('CustomerID').agg({
        'OrderDate': lambda x: (sales_df['OrderDate'].max() - x.max()).days,  # Recency
        'OrderID': 'count',      # Frequency (number of orders)
        'TotalAmount': 'sum'     # Monetary (total spending)
    }).reset_index()
    
    rfm_df.columns = ['CustomerID', 'Recency', 'Frequency', 'Monetary']
    
    # Merge with customer demographic information
    rfm_df = rfm_df.merge(customers_df, on='CustomerID', how='left')
    
    # Prepare features for clustering algorithm
    clustering_features = ['Recency', 'Frequency', 'Monetary']
    X = rfm_df[clustering_features]
    
    # Preprocess data for clustering
    X = np.log1p(X)  # Log transformation to handle skewed distributions
    scaler = StandardScaler()  # Standardize features to mean=0, std=1
    X_scaled = scaler.fit_transform(X)
    
    # Determine optimal number of clusters using elbow method
    wcss = []  # Within-Cluster Sum of Squares (measure of cluster compactness)
    for i in range(1, 11):
        kmeans = KMeans(n_clusters=i, random_state=42)
        kmeans.fit(X_scaled)
        wcss.append(kmeans.inertia_)  # Inertia = within-cluster sum of squares
    
    # Apply K-means clustering with optimal number of clusters
    optimal_clusters = 4  # Typically chosen from elbow point in WCSS plot
    kmeans = KMeans(n_clusters=optimal_clusters, random_state=42)
    rfm_df['Cluster'] = kmeans.fit_predict(X_scaled)  # Assign cluster labels
    
    # Analyze characteristics of each cluster
    cluster_summary = rfm_df.groupby('Cluster').agg({
        'Recency': 'mean',      # How recently customers purchased
        'Frequency': 'mean',    # How often they purchase
        'Monetary': 'mean',     # How much they spend
        'CustomerID': 'count'   # Number of customers in cluster
    }).round(2)
    
    cluster_summary['Percentage'] = (cluster_summary['CustomerID'] / len(rfm_df) * 100).round(2)
    
    # Visualize clusters in 2D using PCA (dimensionality reduction)
    pca = PCA(n_components=2)  # Reduce to 2 dimensions for visualization
    principal_components = pca.fit_transform(X_scaled)
    rfm_df['PC1'] = principal_components[:, 0]  # First principal component
    rfm_df['PC2'] = principal_components[:, 1]  # Second principal component
    
    return rfm_df, cluster_summary, kmeans

# 4. Data Quality Assessment Framework
"""
Data Quality: Measures how fit data is for its intended use in operations, 
decision making, and planning. Key dimensions: completeness, validity, 
consistency, timeliness, and accuracy.
"""
import pandas as pd
import numpy as np

class DataQualityChecker:
    """
    Comprehensive data quality assessment tool that evaluates multiple
    dimensions of data quality and generates detailed reports.
    """
    
    def __init__(self, df):
        """
        Initialize with the dataframe to be assessed.
        
        Args:
            df (DataFrame): Data to be evaluated for quality
        """
        self.df = df
        self.quality_report = {}  # Store assessment results
    
    def generate_quality_report(self):
        """
        Generate comprehensive data quality assessment report.
        
        Returns:
            dict: Dictionary containing all quality metrics and issues
        """
        report = {
            'basic_info': self._get_basic_info(),      # Dataset overview
            'completeness': self._check_completeness(), # Missing values
            'uniqueness': self._check_uniqueness(),     # Duplicates and unique values
            'consistency': self._check_consistency(),   # Format and pattern consistency
            'validity': self._check_validity(),         # Business rule compliance
            'accuracy': self._check_accuracy()          # Outliers and data correctness
        }
        self.quality_report = report
        return report
    
    def _get_basic_info(self):
        """Get basic dataset information and statistics."""
        return {
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns),
            'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024**2,
            'data_types': self.df.dtypes.to_dict()  # Column data types
        }
    
    def _check_completeness(self):
        """Check for missing values across all columns."""
        missing_data = self.df.isnull().sum()  # Count missing values per column
        missing_percentage = (missing_data / len(self.df)) * 100
        
        return {
            'missing_counts': missing_data.to_dict(),
            'missing_percentage': missing_percentage.to_dict(),
            'columns_with_high_missing': missing_percentage[missing_percentage > 20].index.tolist()
        }
    
    def _check_uniqueness(self):
        """Check for duplicate records and analyze unique values."""
        duplicate_rows = self.df.duplicated().sum()  # Count completely duplicate rows
        unique_stats = {}
        
        for col in self.df.columns:
            unique_count = self.df[col].nunique()  # Count distinct values
            unique_stats[col] = {
                'unique_count': unique_count,
                'unique_percentage': (unique_count / len(self.df)) * 100
            }
        
        return {
            'duplicate_rows': duplicate_rows,
            'duplicate_percentage': (duplicate_rows / len(self.df)) * 100,
            'unique_stats': unique_stats
        }
    
    def _check_consistency(self):
        """Check data consistency in formatting and patterns."""
        consistency_issues = {}
        
        # Analyze text columns for inconsistent formatting
        text_columns = self.df.select_dtypes(include=['object']).columns
        for col in text_columns:
            # Check for mixed case usage in text data
            value_samples = self.df[col].dropna().head(10).tolist()
            consistency_issues[col] = {
                'sample_values': value_samples,
                'has_mixed_case': any(str(x).islower() for x in value_samples) and 
                                any(str(x).isupper() for x in value_samples)
            }
        
        return consistency_issues
    
    def _check_validity(self):
        """Validate data against business rules and constraints."""
        validity_issues = {}
        
        # Example business rule: Age should be between 0 and 120
        if 'Age' in self.df.columns:
            invalid_ages = self.df[(self.df['Age'] < 0) | (self.df['Age'] > 120)]
            validity_issues['Age'] = {
                'invalid_count': len(invalid_ages),
                'invalid_records': invalid_ages.index.tolist()
            }
        
        # Example business rule: Email should follow valid format
        if 'Email' in self.df.columns:
            # Regular expression for basic email validation
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_emails = self.df[~self.df['Email'].str.match(email_pattern, na=False)]
            validity_issues['Email'] = {
                'invalid_count': len(invalid_emails),
                'invalid_records': invalid_emails.index.tolist()
            }
        
        return validity_issues
    
    def _check_accuracy(self):
        """Check data accuracy through statistical validation and outlier detection."""
        accuracy_issues = {}
        
        # Analyze numeric columns for outliers using IQR method
        numeric_columns = self.df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            # Calculate Interquartile Range (IQR) for outlier detection
            Q1 = self.df[col].quantile(0.25)  # First quartile (25th percentile)
            Q3 = self.df[col].quantile(0.75)  # Third quartile (75th percentile)
            IQR = Q3 - Q1                     # Interquartile range
            
            # Define outlier boundaries (1.5 * IQR from quartiles)
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Identify outliers
            outliers = self.df[(self.df[col] < lower_bound) | (self.df[col] > upper_bound)]
            
            accuracy_issues[col] = {
                'outlier_count': len(outliers),
                'outlier_percentage': (len(outliers) / len(self.df)) * 100,
                'outlier_bounds': {'lower': lower_bound, 'upper': upper_bound}
            }
        
        return accuracy_issues

# 5. Automated Reporting with PDF Generation
"""
Automated Reporting: Process of generating regular reports automatically
without manual intervention, ensuring consistency and timeliness.
"""
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import pandas as pd

def generate_analytical_report(df, output_path):
    """
    Generate professional PDF analytical report from data.
    
    Args:
        df (DataFrame): Data to include in the report
        output_path (str): File path to save the PDF report
    """
    
    # Create PDF document with letter size
    doc = SimpleDocTemplate(output_path, pagesize=letter)
    styles = getSampleStyleSheet()  # Pre-defined styling options
    story = []  # Container for report elements
    
    # Add title to the report
    title = Paragraph("Sales Analytics Report", styles['Title'])
    story.append(title)
    
    # Add executive summary
    summary_text = """
    This report provides a comprehensive analysis of sales performance, 
    customer behavior, and product trends based on the latest data.
    Key insights include seasonal patterns, top-performing products, 
    and customer segmentation analysis to inform strategic decisions.
    """
    summary = Paragraph(summary_text, styles['Normal'])
    story.append(summary)
    
    # Create key metrics table
    metrics_data = [
        ['Metric', 'Value', 'Change vs Previous Period'],  # Table headers
        ['Total Sales', f"${df['TotalAmount'].sum():,.2f}", '+12.5%'],
        ['Average Order Value', f"${df['TotalAmount'].mean():.2f}", '+3.2%'],
        ['Total Customers', f"{df['CustomerID'].nunique():,}", '+8.1%'],
        ['Orders Processed', f"{len(df):,}", '+15.3%']
    ]
    
    metrics_table = Table(metrics_data)
    metrics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),  # Header background
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),  # Header text color
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align all cells
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Bold header font
        ('FONTSIZE', (0, 0), (-1, 0), 14),  # Larger header font size
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),  # Header padding
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),  # Data row background
        ('GRID', (0, 0), (-1, -1), 1, colors.black)  # Add grid lines
    ]))
    story.append(metrics_table)
    
    # Create top products table
    top_products = df.groupby('ProductName').agg({
        'TotalAmount': 'sum',
        'Quantity': 'sum', 
        'OrderID': 'count'
    }).nlargest(10, 'TotalAmount').reset_index()  # Get top 10 by sales
    
    products_data = [['Product', 'Total Sales', 'Quantity', 'Orders']]  # Headers
    for _, row in top_products.iterrows():
        products_data.append([
            row['ProductName'],
            f"${row['TotalAmount']:,.2f}",  # Format as currency
            f"{row['Quantity']:,}",         # Format with thousands separator
            f"{row['OrderID']:,}"           # Format with thousands separator
        ])
    
    products_table = Table(products_data)
    products_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightblue),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    story.append(products_table)
    
    # Build and save the PDF document
    doc.build(story)
    print(f"Report generated successfully: {output_path}")

# 6. Web Scraping for Market Data
"""
Web Scraping: Automated extraction of data from websites for analysis,
monitoring, or data collection purposes.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

class MarketDataScraper:
    """
    Web scraper for collecting competitor pricing and product information
    from e-commerce websites.
    """
    
    def __init__(self):
        """Initialize scraper with session and headers."""
        self.session = requests.Session()
        # Set user agent to mimic real browser (avoids blocking)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_competitor_prices(self, urls):
        """
        Scrape product and pricing data from multiple competitor URLs.
        
        Args:
            urls (list): List of website URLs to scrape
            
        Returns:
            DataFrame: Combined product data from all sources
        """
        all_products = []
        
        for url in urls:
            try:
                # Send HTTP GET request to the webpage
                response = self.session.get(url)
                # Parse HTML content using BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract product elements (CSS selector will vary by website)
                products = soup.find_all('div', class_='product-item')
                
                for product in products:
                    product_data = {
                        'competitor': self._extract_competitor_name(url),
                        'product_name': self._extract_text(product, '.product-name'),
                        'price': self._extract_price(product, '.price'),
                        'rating': self._extract_rating(product, '.rating'),
                        'availability': self._extract_availability(product, '.stock'),
                        'scraped_date': pd.Timestamp.now()  # Timestamp for data freshness
                    }
                    all_products.append(product_data)
                
                time.sleep(1)  # Be respectful - don't overwhelm servers
                
            except Exception as e:
                print(f"Error scraping {url}: {str(e)}")
                continue  # Continue with next URL if one fails
        
        return pd.DataFrame(all_products)
    
    def _extract_competitor_name(self, url):
        """
        Extract competitor name from website URL.
        
        Args:
            url (str): Website URL
            
        Returns:
            str: Competitor name
        """
        domain = url.split('//')[-1].split('/')[0]  # Extract domain
        return domain.replace('www.', '').split('.')[0].title()
    
    def _extract_text(self, element, selector):
        """
        Extract text content from HTML element using CSS selector.
        
        Args:
            element: BeautifulSoup element to search within
            selector (str): CSS selector string
            
        Returns:
            str: Extracted text or None if not found
        """
        found = element.select_one(selector)  # Find first matching element
        return found.get_text(strip=True) if found else None
    
    def _extract_price(self, element, selector):
        """
        Extract and clean price from HTML element.
        
        Args:
            element: BeautifulSoup element to search within  
            selector (str): CSS selector for price element
            
        Returns:
            float: Cleaned price value or None
        """
        price_text = self._extract_text(element, selector)
        if price_text:
            # Remove currency symbols and non-numeric characters
            import re
            clean_price = re.sub(r'[^\d.]', '', price_text)  # Keep only digits and decimal
            return float(clean_price) if clean_price else None
        return None
    
    def _extract_rating(self, element, selector):
        """
        Extract product rating from HTML element.
        
        Args:
            element: BeautifulSoup element to search within
            selector (str): CSS selector for rating element
            
        Returns:
            float: Rating value or None
        """
        rating_text = self._extract_text(element, selector)
        if rating_text:
            # Extract first number found in rating text
            import re
            numbers = re.findall(r'\d+\.?\d*', rating_text)  # Find all numbers
            return float(numbers[0]) if numbers else None
        return None
    
    def _extract_availability(self, element, selector):
        """
        Extract stock availability information.
        
        Args:
            element: BeautifulSoup element to search within
            selector (str): CSS selector for availability element
            
        Returns:
            str: 'In Stock' or 'Out of Stock'
        """
        availability = self._extract_text(element, selector)
        # Simple logic to determine stock status
        return 'In Stock' if availability and 'stock' in availability.lower() else 'Out of Stock'