network_analytics_fields = {
    "report": {
        "report_type": "network_analytics",
        "report_interval": "last_30_days",
        "columns": [
            "hour",
            "insertion_order_id",
            "line_item_id",
            "campaign_id",
            "advertiser_id",
            "pixel_id",
            "imps",
            "imps_viewed",
            "clicks",
            "cost",
            "cpm",
            "cpm_including_fees",
            "revenue",
            "revenue_including_fees",
            "total_convs",
            'geo_country'
        ],
        'filters':[{'geo_country': 'FR'}],
        "groups": ["advertiser_id", "hour"],
        "format": "csv"
    }
}

segment_load_fields = {
    "report": {
        "report_type": "segment_load",
        "columns": ["segment_id",
                    "segment_name",
                    "month",
                    "total_loads",
                    "monthly_uniques",
                    "avg_daily_uniques"],
        "format": "csv",
        "report_interval": "month_to_date",
        "groups": ["segment_id", "month"],
        "orders": ["month"],
    }}
