var NotificationClient = (function(){
    var self = {};

    self.success = function(message){
        $.notify(message, "success");
    };

    self.warn = function(message){
        console.warn(message);
        $.notify(message, "warn");
    };

    self.error = function(message){
        console.error(message);
        $.notify(message, "error");
    };

    return self;
})();

var APIClient = (function(){
    var self = {};

    var verify_response = function(response, callback_function){
        if( response.status == "ok" ){
            callback_function(response.data);
        } else {
            NotificationClient.error(response.message);
        }
    };

    var get_request = function(url){
        return function(callback_function){
            $.get( url )
                .done(function( response ){
                    verify_response(response, callback_function);
                })
                .fail(function( response ){
                    NotificationClient.error(response.message);
                });
        };
    };

    self.get_help = get_request("help");
    self.get_status = get_request("status");
    self.get_parameters = get_request("parameters");
    self.get_statistics = get_request("statistics");
    self.start = get_request("start");
    self.stop = get_request("stop");

    self.set_parameters = function(json_string, callback_function){
        $.ajax({
            url: 'parameters',
            type: 'POST',
            data: json_string,
            dataType: 'json',
            contentType: "application/json",
            success: function(response){
                verify_response(response, callback_function);
            },
            error: function(response){
                NotificationClient.error(response.message);
            }
        });
    };

    return self;
})();

var display_process_data = function(){
    APIClient.get_status(function(data){

        $("#processor_name").text(data.processor_name);

        if(data.is_running){
            $("#is_running_value").text("Running");
        }else{
            $("#is_running_value").text("Stopped");
        }

        $("#process_parameters").val(JSON.stringify(data.parameters, null, 4));
    });

    APIClient.get_statistics(function(data){

        var statistics_html = '';
        for(var value in data.statistics){
            statistics_html += "<li>" + value + " = " + data.statistics[value];
        }

        $("#processor_statistics").html(statistics_html);
    });
};


$( document ).ready(function() {

    $("button#start_processor").click(function(){
        APIClient.start(function(){
            NotificationClient.success("Processor started.")
            display_process_data();
        })
    });

    $("button#stop_processor").click(function(){
        APIClient.stop(function(){
            NotificationClient.success("Processor stopped.")
            display_process_data();
        })
    });

    $("button#update_parameters").click(function(){
        var json_string = $("#process_parameters").val();
        APIClient.set_parameters(json_string, function(){
            NotificationClient.success("Parameters updated.");
            display_process_data();
        });
    });

    APIClient.get_help(function(data){
        $("#processor_help").text(data);
    });

    display_process_data();
});