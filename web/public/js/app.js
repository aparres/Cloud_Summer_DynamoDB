$(document).ready(function() {
  $("#version").html("v0.14");
  
  $("#searchbutton").click( function (e) {
    displayModal();
  });
  
  $("#searchfield").keydown( function (e) {
    if(e.keyCode == 13) {
      displayModal();
    }	
  });
  
  function displayModal() {
    $(  "#myModal").modal('show');

    for(i=0; i < 4; i++) {
       $("#photo"+i).html("");
    }
    
    $("#status").html("Searching...");
    $("#dialogtitle").html("Search for: "+$("#searchfield").val());
    $("#previous").hide();
    $("#next").hide();
    $.getJSON('/search/' + $("#searchfield").val() , function(data) {
      renderQueryResults(data,0);
    });
  }
  
  $("#next").click( function(e) {

  });
  
  $("#previous").click( function(e) {
  
  });

  function renderQueryResults(data) {
    
    if (data.error != undefined) {
      $("#status").html("Error: "+data.error);
    } else {
      $("#status").html(""+data.num_results+" result(s)");
          
      if(data.num_results > 0) {
          for(i=0; i < 4 && i < data.num_results; i++) {
              $("#photo"+i).html("<img width='180px' src='"+data.results[i]+"'></img>")
          }
      }    
          
      $("#next").show();
      $("#previous").show();
      
     }
   }
});
