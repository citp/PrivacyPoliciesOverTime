<?php

    //echo "Start";

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

header("Content-Type: text/plain");

//https://stackoverflow.com/questions/1755144/how-to-validate-domain-name-in-php
function is_valid_domain_name($domain_name)
{
    return (preg_match("/^(https?:\/\/)?([a-z\d](-*[a-z\d])*)(\.([a-z\d](-*[a-z\d])*))*$/i", $domain_name) //valid chars check
            && preg_match("/^.{1,253}$/", $domain_name) //overall length check
            && preg_match("/^[^\.]{1,63}(\.[^\.]{1,63})*$/", $domain_name)   ); //length of each label
}

function is_valid_interval($interval) {
    return preg_match('/\d\d\d\d_\w/', $interval);
}

function iter_y_s_down($year,$season) {
    
    while($year > 2009) {
        yield array($year,$season);
        if($season == 'B'){
            $season = 'A';
        }
        else{
            $season = 'B';
            $year = $year - 1;
        }
    }
    return;
}

$use_db = true;

if(isset($_GET["domain"]) && isset($_GET["interval"])) {
    if(is_valid_domain_name($_GET["domain"]) && is_valid_interval($_GET["interval"])) {

        if($use_db) {
            $interval=$_GET["interval"];
            $domain=$_GET["domain"];
            $year=(int)substr($interval,0,4);
            $season=substr($interval,5,1);

            $db = new SQLite3('/n/fs/policyphylog/PrivacyPolicyPlagiarism/data/sqlite/policy.sqlite3');

            $first=true;
            foreach(iter_y_s_down($year,$season) as $ys) {
                $year = $ys[0];
                $season = $ys[1];
                $stmt = $db->prepare('SELECT policy_text FROM policy_texts WHERE year==:year AND season==:season AND site_url==:siteurl;');
                $stmt->bindValue(':year', $year, SQLITE3_INTEGER);
                $stmt->bindValue(':season', $season);
                $stmt->bindValue(':siteurl', $domain);
                
                $result = $stmt->execute();
                $resAr = $result->fetchArray();
                if($resAr){
                    
                    if(!$first) {
                        echo("**************************NOTE************************\nPolicy Imputed based on most recent occurance ($year$season)\n******************************************************\n\n");
                    }
                    echo($resAr[0]);
                    break;
                }
                $first=false;
            }
            
        } else {        
            $globstr="/n/fs/privpolicy/data/wayback_crawls/top_10K_1996-2019/text/*_" . $_GET["interval"] . "_*_" . $_GET["domain"] . "_*.txt";
            $matches = glob($globstr);

            foreach ($matches as $match){
                echo file_get_contents($match);
            }
        }

    } else {
        //echo("Failed safety checks");
    }
} else {
    //echo("Not set");
}
?>
