<form action="search_policies.php">
<input type="text" width=300 id="query" name="query"><br>
            
<input type="hidden" id="basedir" name="basedir" value=
<?php
            if(isset($_GET["basedir"]) and ctype_alnum($_GET["basedir"])) {
                echo "\"".$_GET["basedir"]."\"";
            } else {
                echo "\"\"";
            }
            ?>
            ><br>
<input type="submit" value="search"><br>
</form>


<?php

//echo "Start";

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

//header("Content-Type: text/plain");


$testing="";
$testing_re="";

if(isset($_GET["basedir"]) and ctype_alnum($_GET["basedir"]) and $_GET["basedir"] != "") {
    $testing=$_GET["basedir"] . "/";
    $testing_re = $_GET["basedir"] . "\\/";
}

if(isset($_GET["query"]) and strlen($_GET["query"]) < 300 and strlen($_GET["query"]) > 2) {
    $query=$_GET["query"];
    echo("Query: ".htmlspecialchars($query)."<br/>");
    $command="grep -R -P -i " . escapeshellarg($query) . " /n/fs/policyphylog/public_html/".$testing."policies/metrics/";
    $res = shell_exec($command);
    preg_match_all("/^[^:]*?(".$testing_re."policies\\/metrics\\/[^\\:]*):[^\\>]*\\>([^\\<]*)\\</m", $res,$matches,PREG_SET_ORDER);
    foreach($matches as $match) {
        echo('<a href="' . $match[1] . '" target="_blank">' . $match[2] . '</a><br/><br/>');
    }       
} else {

}
?>
