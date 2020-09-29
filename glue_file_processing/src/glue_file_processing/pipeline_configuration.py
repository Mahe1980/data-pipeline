class ConfigurationException(Exception):
    def __init__(self, msg: str):
        self.msg = msg
        super().__init__()


CONFIGURATION = {'delimiter': ',', 'file_name_timestamp': '%Y%m%d%H%M%S',
                 "schema": [
                     {"name": "asofdate", "type": "DATE", "is_calculated": "true"},
                     {"name": "Portfolio_Name", "type": "STRING"},
                     {"name": "Portfolio_ID", "type": "INT"},
                     {"name": "Asset_LienType", "type": "STRING"},
                     {"name": "Issuer_Name", "type": "STRING"},
                     {"name": "Asset_Name", "type": "STRING"},
                     {"name": "AssetType_Name", "type": "STRING"},
                     {"name": "Asset_PrimaryIdentifier", "type": "STRING"},
                     {"name": "Asset_SandPRating_Name", "type": "STRING"},
                     {"name": "Asset_SandPRatingSort", "type": "INT"},
                     {"name": "Asset_Rate", "type": "STRING"},
                     {"name": "Quantity", "type": "DECIMAL"},
                     {"name": "Asset_MaturityDate", "type": "TIMESTAMP"},
                     {"name": "Asset_Currency_ISOCode", "type": "STRING"},
                     {"name": "ParAmountNZD", "type": "DECIMAL"},
                     {"name": "MarketValueAUD", "type": "DECIMAL"},
                     {"name": "ParAmountAUD", "type": "DECIMAL"},
                     {"name": "MarketValueNZD", "type": "DECIMAL"},
                     {"name": "ParAmount", "type": "DECIMAL"},
                     {"name": "ParAmountRC", "type": "DECIMAL"},
                     {"name": "MarketValueRC", "type": "DECIMAL"},
                     {"name": "Issuer_MoodysOutlook", "type": "STRING"},
                     {"name": "Asset_Seniority_Name", "type": "STRING"},
                     {"name": "Issuer_ID", "type": "INT"},
                     {"name": "Asset_ID", "type": "INT"},
                     {"name": "Asset_LIN", "type": "STRING"},
                     {"name": "Asset_IsFixed", "type": "BOOLEAN"},
                     {"name": "Asset_FixedRate", "type": "DECIMAL"},
                     {"name": "Asset_FloatingSpread", "type": "DECIMAL"},
                     {"name": "Asset_CUSIP", "type": "STRING"},
                     {"name": "Asset_CurrentGlobalAmount", "type": "DECIMAL"},
                     {"name": "Asset_LoanXID", "type": "STRING"},
                     {"name": "Asset_SpreadBps", "type": "DECIMAL"},
                     {"name": "MarketValue", "type": "DECIMAL"},
                     {"name": "PurchasePrice", "type": "DECIMAL"},
                     {"name": "MarketValuePending", "type": "DECIMAL"},
                     {"name": "MarketValuePendingRC", "type": "DECIMAL"},
                     {"name": "MarketValuePendingUSD", "type": "DECIMAL"},
                     {"name": "MarketValueUSD", "type": "DECIMAL"},
                     {"name": "ParAmountUSD", "type": "DECIMAL"},
                     {"name": "Asset_MarkPrice", "type": "DECIMAL"},
                     {"name": "Portfolio_AbbrevName", "type": "STRING"},
                     {"name": "Asset_SandPOutlook", "type": "STRING"},
                     {"name": "Asset_SandPWatch", "type": "STRING"},
                     {"name": "Asset_SandPRecovery", "type": "STRING"},
                     {"name": "CreditID", "type": "INT"},
                     {"name": "Asset_MoodysRating_Name", "type": "STRING"},
                     {"name": "MoodysIndustry_Name", "type": "STRING"},
                     {"name": "Asset_MoodysWatch", "type": "STRING"},
                     {"name": "Asset_MoodysOutlook", "type": "STRING"},
                     {"name": "Asset_MoodysRating_Sort", "type": "INT"},
                     {"name": "Asset_BidPrice", "type": "DECIMAL"},
                     {"name": "Asset_AskPrice", "type": "DECIMAL"},
                     {"name": "Issuer_MoodyRating", "type": "STRING"},
                     {"name": "Issuer_MoodyRatingSortOrder", "type": "INT"},
                     {"name": "Issuer_SandPRatingSortOrder", "type": "INT"},
                     {"name": "Issuer_ParentCompany", "type": "STRING"},
                     {"name": "Issuer_SandPRating", "type": "STRING"},
                     {"name": "ParAmountSettled", "type": "DECIMAL"},
                     {"name": "ParAmountRCSettled", "type": "DECIMAL"},
                     {"name": "MarketValueGBP", "type": "DECIMAL"},
                     {"name": "MarketValueEUR", "type": "DECIMAL"},
                     {"name": "ParAmountEUR", "type": "DECIMAL"},
                     {"name": "ParAmountGBP", "type": "DECIMAL"},
                     {"name": "PortfolioType", "type": "STRING"},
                     {"name": "PoolFactorBBG", "type": "DECIMAL"},
                     {"name": "FactoredMarketValue", "type": "DECIMAL"},
                     {"name": "Asset_ISIN", "type": "STRING"},
                     {"name": "PercentOfNAV", "type": "DECIMAL"},
                     {"name": "Asset_SandPRating_Type", "type": "STRING"},
                     {"name": "Asset_FitchRating", "type": "STRING"},
                     {"name": "Issuer_MoodyRatingType", "type": "STRING"},
                     {"name": "DiscountMargin", "type": "DECIMAL"},
                     {"name": "Asset_IssueAmount", "type": "DECIMAL"},
                     {"name": "Asset_AgentBankName", "type": "STRING"},
                     {"name": "Asset_MoodysAdjRatingFactor", "type": "INT"},
                     {"name": "Asset_MoodysAdjRating", "type": "STRING"},
                     {"name": "Asset_MoodysRecoveryRating", "type": "STRING"},
                     {"name": "Asset_CovLiteTranche", "type": "STRING"},
                     {"name": "Asset_CovLiteFacility", "type": "STRING"},
                     {"name": "Asset_FloatingSpreadCap", "type": "DECIMAL"},
                     {"name": "Issuer_FitchIDRRating", "type": "STRING"},
                     {"name": "Issuer_FitchIDRType", "type": "STRING"},
                     {"name": "Issuer_FitchOutlookRating", "type": "STRING"},
                     {"name": "Issuer_FitchWatchRating", "type": "STRING"},
                     {"name": "Issuer_SandPOutlookRating", "type": "STRING"},
                     {"name": "WSOID", "type": "INT"},
                     {"name": "Issuer_FitchIndustry_Name", "type": "STRING"},
                     {"name": "WSO_SandPIndustry_Name", "type": "STRING"},
                     {"name": "Asset_MinSellPrice", "type": "DECIMAL"},
                     {"name": "SellPriority", "type": "STRING"},
                     {"name": "Asset_LoanAliases", "type": "STRING"},
                     {"name": "Timetable", "type": "STRING"},
                     {"name": "Issuer_IssuerLimitFirstLien", "type": "DECIMAL"},
                     {"name": "Issuer_IssuerLimitSecondLien", "type": "DECIMAL"},
                     {"name": "Asset_CountryOfIssue_Name", "type": "STRING"},
                     {"name": "MoodysCorporateIndustry_Name", "type": "STRING"},
                     {"name": "SandPIndustry_Name", "type": "STRING"},
                     {"name": "DiscountMarginAsk", "type": "DECIMAL"},
                     {"name": "DiscountMargin3yr", "type": "DECIMAL"},
                     {"name": "YieldToThreeYears", "type": "DECIMAL"},
                     {"name": "YieldToMaturity", "type": "DECIMAL"},
                     {"name": "LotLastUpdatedDate", "type": "TIMESTAMP"},
                     {"name": "Asset_IsDefaulted", "type": "BOOLEAN"},
                     {"name": "YTW", "type": "DECIMAL"},
                     {"name": "STW", "type": "DECIMAL"},
                     {"name": "LTM_EBITDA", "type": "DECIMAL"},
                     {"name": "Asset_ICGInternalRatingNumericID", "type": "INT"},
                     {"name": "AssetRecommendation_UK", "type": "STRING"},
                     {"name": "Issuer_CreditScore_US", "type": "STRING"},
                     {"name": "Issuer_CreditScore_UK", "type": "DECIMAL"},
                     {"name": "LTM_Revenue", "type": "DECIMAL"},
                     {"name": "LTM_TotalLeverage", "type": "DECIMAL"},
                     {"name": "Issuer_WatchlistStatusID_US", "type": "STRING"},
                     {"name": "Issuer_WatchlistStatusID_UK", "type": "STRING"},
                     {"name": "Issuer_WatchlistStatusID_AUS", "type": "STRING"},
                     {"name": "HoldStatusID", "type": "STRING"},
                     {"name": "PortfolioGroup", "type": "STRING"},
                     {"name": "Portfolio_BusinessUnit", "type": "STRING"},
                     {"name": "VirtusID", "type": "INT"},
                     {"name": "VirtusAssetID", "type": "INT"},
                     {"name": "WSOAssetID", "type": "INT"},
                     {"name": "Asset_SpreadFloorBps", "type": "INT"},
                     {"name": "BBGDuration", "type": "DECIMAL"},
                     {"name": "CombinedIndustry_Name", "type": "STRING"},
                     {"name": "LTM_FirstLienLeverage", "type": "DECIMAL"},
                     {"name": "LTM_SeniorSecuredLeverage", "type": "DECIMAL"},
                     {"name": "Credit_Name", "type": "STRING"},
                     {"name": "FirstLienLeverageOnly", "type": "BOOLEAN"},
                     {"name": "SeniorSecuredLeverageOnly", "type": "BOOLEAN"},
                     {"name": "FitchRatingEffectiveDate", "type": "TIMESTAMP"},
                     {"name": "FitchRecoveryRating", "type": "STRING"},
                     {"name": "MoodysRatingEffectiveDate", "type": "TIMESTAMP"},
                     {"name": "MoodysLGDRating", "type": "STRING"},
                     {"name": "SandPRatingEffectiveDate", "type": "TIMESTAMP"},
                     {"name": "Asset_MarkCloseDate", "type": "TIMESTAMP"},
                     {"name": "FitchRecoveryRate", "type": "STRING"},
                     {"name": "MoodysLGDRate", "type": "STRING"},
                     {"name": "SandPRecoveryRate", "type": "STRING"},
                     {"name": "Asset_SandPRecoveryRateAAA", "type": "INT"},
                     {"name": "Issuer_Analyst_UK", "type": "STRING"},
                     {"name": "Issuer_Analyst_AUS", "type": "STRING"},
                     {"name": "Issuer_Analyst_US", "type": "STRING"},
                     {"name": "Credit_Analyst", "type": "STRING"},
                     {"name": "Credit_SecondaryAnalyst", "type": "STRING"},
                     {"name": "SubIndustry", "type": "STRING"},
                     {"name": "Industry", "type": "STRING"},
                     {"name": "IndustryGroup", "type": "STRING"},
                     {"name": "Sector", "type": "STRING"},
                     {"name": "SandPCombinedIndustry_AbbrevName", "type": "STRING"},
                     {"name": "MoodysCombinedIndustry_AbbrevName", "type": "STRING"},
                     {"name": "Issuer_Abbrev", "type": "STRING"},
                     {"name": "BackOfficeSystem", "type": "STRING"},
                     {"name": "ParAmountWeight", "type": "DECIMAL"},
                     {"name": "PercentOfFundMVRC", "type": "DECIMAL"},
                     {"name": "NextCallDate", "type": "TIMESTAMP"},
                     {"name": "NextCallPrice", "type": "DECIMAL"},
                     {"name": "IssuerAlias", "type": "STRING"},
                     {"name": "PrimarySponsor", "type": "STRING"},
                     {"name": "OtherSponsors", "type": "STRING"},
                     {"name": "InformationStatus_UKUS", "type": "STRING"},
                     {"name": "InformationStatus_UKUS_Comments", "type": "STRING"},
                     {"name": "InformationStatus_AUS", "type": "STRING"},
                     {"name": "InformationStatus_AUS_Comments", "type": "STRING"},
                     {"name": "row_index", "type": "INT", "is_calculated": "true"},
                     {"name": "correlation_id", "type": "STRING", "is_calculated": "true"},
                     {"name": "confidence_level", "type": "DECIMAL(3,2)", "is_calculated": "true"}

                 ],
                 'rules': [
                     {"blank": {
                         "columns_to_check": ["Asset_CountryOfIssue_Name", "SubIndustry", "IndustryGroup", "Sector",
                                              "Asset_MoodysRating_Name"],
                         "threshold": 0, "weight": 0}},
                     {"column_count": {"expected_column_count": 163, "threshold": 1.0, "weight": 0.4}},
                     {"column_names": {"threshold": 1.0, "weight": 0.6}},
                     {"outliers": {"columns_to_check": ["PurchasePrice", "Asset_BidPrice", "Asset_AskPrice",
                                                        "Asset_MarkPrice"], "threshold": 0, "weight": 0}},
                     {"data_types": {"columns_to_check": [], "threshold": 0, "weight": 0}},
                     {"condition_check": {"condition_config":
                                          [{'condition_column': 'AssetType_Name',
                                            'condition_operator': 'regex',
                                            'condition_operand': 'Loan[0-9]*',
                                            'test_operator': 'column_equality',
                                            'test_column': 'Asset_PrimaryIdentifier',
                                            'test_operand': 'Asset_LoanXID'},
                                           {'condition_column': 'AssetType_Name',
                                            'condition_operator': 'regex',
                                            'condition_operand': '(Bond[0-9]*|ABS)',
                                            'test_operator': 'column_equality',
                                            'test_column': 'Asset_PrimaryIdentifier',
                                            'test_operand': 'Asset_ISIN'}],
                                          "threshold": 0,
                                          "weight": 0}
                      },
                     {"primary_id_variation": {"primary_keys": ["Asset_PrimaryIdentifier"],
                                               "descriptive_keys": ["Asset_CountryOfIssue_Name", "Asset_MaturityDate",
                                                                    "Issuer_MoodyRating", "PortfolioType",
                                                                    "Asset_Currency_ISOCode"],
                                               "threshold": 0, "weight": 0}},
                     {"duplicates": {"columns_to_check": [], "threshold": 0, "weight": 0}}
                 ],
                 "error_reporting": {
                     "asofdate": "asofdate",
                     "business_unit": "Portfolio_BusinessUnit",
                     "portfolio": "Portfolio_Name",
                     "company": "Issuer_Name"
                 }
                 }
