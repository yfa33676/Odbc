# ODBC接続
$OdbcConnection = New-Object System.Data.Odbc.OdbcConnection
# SQLコマンド
$OdbcCommand = New-Object System.Data.Odbc.OdbcCommand
$OdbcCommand.Connection = $OdbcConnection

function Open-OdbcConnection {
    param(
        [parameter(Mandatory, Position = 0)]
        [string]$DSN,
        [parameter(Mandatory, Position = 1)]
        [System.Management.Automation.PSCredential]$Credential
    )
    process{
        $UID = $Credential.UserName
        $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($Credential.Password)
        $PWD = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($BSTR)
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($BSTR)

        $OdbcConnection.ConnectionString = "DSN=$DSN;UID=$UID;PWD=$PWD"
        $OdbcConnection.Open()
    }
}

function Get-OdbcSchema {
    param(
        [parameter(Mandatory, Position = 0)]
        [validateset("Columns", "Tables", "Views")]
        [string]$CollectionName,
        [string]$SchemaName,
        [string]$TableName
    )
    process{
        $OdbcConnection.GetSchema($CollectionName, ($OdbcConnection.Database, $SchemaName, $TableName))
    }
}

function Get-OdbcConnection {
    process{
        $OdbcConnection
    }
}

function Close-OdbcConnection {
    process{
        $OdbcConnection.Close()
    }
}

function Set-OdbcCommand {
    param(
        [parameter(ValueFromPipeline, Position = 0)]
        [string]$CommandText,
        [switch]$Transaction,
        [switch]$Commit,
        [switch]$Rollback
    )
    process{
        if($Transaction){
            $OdbcCommand.Transaction = $OdbcConnection.BeginTransaction()
        } elseif($Commit){
            $OdbcCommand.Transaction.Commit()
        } elseif($Rollback){
            $OdbcCommand.Transaction.Rollback()
        } elseif($CommandText) {
            # SQLコマンド
            $OdbcCommand.CommandText = $CommandText
        } else {
            # SQLコマンド
            $OdbcCommand.CommandText = Read-Host "CommandText"
        }		
    }
}

function Invoke-OdbcCommand{
    param(
        [parameter(ValueFromPipeline, Position = 0)]
        [string]$CommandText,
        [switch]$NonQuery
    )
    process{
        if($CommandText){
            Set-OdbcCommand -CommandText $CommandText
        }
        if(!$OdbcCommand.CommandText){
            Set-OdbcCommand
        }
        if($NonQuery){
            $OdbcCommand.ExecuteNonQuery()
        } else {
            try {
                $OdbcDataReader = $OdbcCommand.ExecuteReader()
                while($OdbcDataReader.Read()){
                    $PSCustomObject = New-Object PSCustomObject
                    for($i = 0; $i -LT $OdbcDataReader.FieldCount; $i++){
                        $PSCustomObject | Add-Member -MemberType NoteProperty -Name $OdbcDataReader.GetName($i) -Value $OdbcDataReader.Item($i)
                    }
                    $PSCustomObject | Write-Output
                }
            } catch{
            } finally {
                $OdbcDataReader.Close()
            }
        }
    }
}

function Get-OdbcCommand {
    process{
        # SQLコマンド
        $OdbcCommand
    }
}

function ConvertTo-Sql {
    [OutputType([string])]
    param(
        [parameter(ValueFromPipeline)]
        [PSCustomObject]$InputObject,
        [switch]$NoColumnName,
        [switch]$Trim,
        [parameter(Mandatory)]
        [string]$TableName,
        [validateset("Insert", "Update", "Delete", "Select")]
        [string]$StatementType = "Insert",
        [string[]]$SetProperty,
        [string[]]$WhereProperty,
        [string]$Delimiter = ""
    )
    process{
        $Properties = @($InputObject.PSObject.Properties)
        if($SetProperty){
            $SetProperties = @($Properties | Where-Object {$_.Name -In $SetProperty})
        } else {
            $SetProperties = @($Properties)
        }
        if($WhereProperty){
            $WhereProperties = @($Properties | Where-Object {$_.Name -In $WhereProperty})
        } else {
            $WhereProperties = @($Properties)
        }
        if($StatementType -EQ "Insert"){
            $Statement = "INSERT INTO"
        } elseif($StatementType -EQ "Update"){
            $Statement = "UPDATE"
        } elseif($StatementType -EQ "Delete"){
            $Statement = "DELETE FROM"
        } elseif($StatementType -EQ "Select"){
            $Statement = "SELECT * FROM"
        }
        $Statement += " "
        $Statement += $TableName
        if($StatementType -EQ "Insert"){
            if(!$NoColumnName){
                $Statement += "("
                for($i = 0; $i -LT $Properties.Count; $i++){
                    if($i -NE 0){
                        $Statement += ","
                    }
                    $Statement += $Properties[$i].Name
                }
                $Statement += ")"
            }
            $Statement += " "
            $Statement += "VALUES"
            $Statement += "("
            for($i = 0; $i -LT $Properties.Count; $i++){
                if($i -NE 0){
                    $Statement += ","
                }
                if($Properties[$i].Value -is [string]){
                    $Statement += "'"
                    if($Trim){
                        $Statement += $Properties[$i].Value.Trim()
                    } else {
                        $Statement += $Properties[$i].Value
                    }
                    $Statement += "'"
                } else {
                    $Statement += $Properties[$i].Value
                }
            }
            $Statement += ")"
        }
        if($StatementType -EQ "Update"){
            $Statement += " "
            $Statement += "SET"
            $Statement += " "
            for($i = 0; $i -LT $SetProperties.Count; $i++){
                if($i -NE 0){
                    $Statement += ","
                }
                $Statement += $SetProperties[$i].Name
                $Statement += " = "
                if($SetProperties[$i].Value -is [string]){
                    $Statement += "'"
                    if($Trim){
                        $Statement += $SetProperties[$i].Value.Trim()
                    } else {
                        $Statement += $SetProperties[$i].Value
                    }
                    $Statement += "'"
                } else {
                    $Statement += $SetProperties[$i].Value
                }
            }
        }
        
        if($StatementType -EQ "Update" -Or $StatementType -EQ "Delete" -or $StatementType -EQ "Select"){
            $Statement += " "
            $Statement += "WHERE"
            $Statement += " "
            for($i = 0; $i -LT $WhereProperties.Count; $i++){
                if($i -NE 0){
                    $Statement += " AND "
                }
                $Statement += $WhereProperties[$i].Name
                $Statement += " = "
                if($WhereProperties[$i].Value -is [string]){
                    $Statement += "'"
                    if($Trim){
                        $Statement += $WhereProperties[$i].Value.Trim()
                    } else {
                        $Statement += $WhereProperties[$i].Value
                    }
                    $Statement += "'"
                } else {
                    $Statement += $WhereProperties[$i].Value
                }
            }
        }
        
        $Statement += $Delimiter
        $Statement | Write-Output
    }
}