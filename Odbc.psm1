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
        [string]$UID,
        [parameter(Mandatory, Position = 2)]
        [string]$PWD
    )
    process{
        $OdbcConnection.ConnectionString = "DSN=$DSN;UID=$UID;PWD=$PWD"
        $OdbcConnection.Open()
        $OdbcConnection
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
        [parameter(Position = 0)]
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
