$jfrog_pypi_uri = "https://icg.jfrog.io/icg/PyPi"
$package_version = "6.0.40"
$lambda_packages = "icg-dp-core", "icg-dp-loggings"
$glue_packages = "icg-dp-core", "icg-dp-loggings", "icg-dp-file-operations", "icg-dp-data-quality",
                 "icg-dp-glue-catalog", "icg-dp-serializer", "icg-dp-hashing", "icg-dp-sns", "icg-dp-tagger"
try
{
    $user, $pass = $env:JFROG_USERNAME_AND_API_KEY.split(':')
    $pass = $pass | ConvertTo-SecureString -AsPlainText -Force
    $cred = New-Object Management.Automation.PSCredential ($user, $pass)

    [Net.ServicePointManager]::SecurityProtocol = "tls12, tls11, tls"

    function DownloadPackage($package_name, $package_type, $output_dir)
    {
        $package_file_name = $package_name.replace("-", "_")
        if ($package_type -eq "whl"){
            $package_file_name = "$package_file_name-$package_version-py3-none-any.whl"
        }
        else
        {
            $package_file_name = "$package_file_name-$package_version-py3.6.egg"
        }
        $uri = "$jfrog_pypi_uri/$package_name/$package_version/$package_file_name"
        Write-Output $uri
        Invoke-WebRequest -Uri $uri -Credential $cred -OutFile "$output_dir/$package_file_name"
    }

    # Lambda
    md "lambda_dist" -ErrorAction SilentlyContinue
    foreach ($lambda_package in $lambda_packages) {
        DownloadPackage $lambda_package "whl" "lambda_dist"
    }

    # Glue
    md "dist_glue" -ErrorAction SilentlyContinue
    foreach ($glue_package in $glue_packages) {
        DownloadPackage $glue_package "egg" "dist_glue"
    }
}
catch
{
    Write-Error -Exception $_.Exception
}
