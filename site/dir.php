
<?php
	function scan_dir($dir) {
	    $ignored = array('.', '..', '.svn', '.htaccess');

	    $files = array();
	    foreach (scandir($dir) as $file) {
	        if (in_array($file, $ignored)) continue;
					if (strtolower(substr($file, strrpos($file, '.') + 1)) == 'csv')
	        	$files[$file] = filemtime($dir . '/' . $file);
	    }

	    arsort($files);
	    $files = array_keys($files);

	    return ($files) ? $files : false;
	}

	if (isset($_POST['callFunc1'])) {
      echo scan_dir($_POST['callFunc1'])[0];
  }

?>
