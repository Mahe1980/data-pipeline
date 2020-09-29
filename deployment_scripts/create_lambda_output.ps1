$package_version = "6.0.40"
$lambda_whl_filename = "icg_dp_lambda_file_processing_trigger-1.0.0-py3-none-any.whl"
try
{
    md "dist_lambda" -ErrorAction SilentlyContinue
    copy lambda_file_processing_trigger\src\main.py dist_lambda
    copy lambda_file_processing_trigger\src\__init__.py  dist_lambda

    $core_whl_filename = "icg_dp_core-$package_version-py3-none-any.whl"
    $loggings_whl_filename = "icg_dp_loggings-$package_version-py3-none-any.whl"

    pip install --upgrade "lambda_dist\$core_whl_filename" "lambda_dist\$loggings_whl_filename" "lambda_file_processing_trigger\src\dist\$lambda_whl_filename" -t dist_lambda
}
catch
{
    Write-Error -Exception $_.Exception
}
