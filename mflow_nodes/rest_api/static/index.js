var NotificationClient = (function(){
    var self = {};

    self.success = function(message){
        $.notify(message, "success");
    };

    self.warn = function(message){
        console.warn(message);
        $.notify(message, "warn");
    };

    self.error = function(response){
        console.error(response.message);
        $.notify(response.message, "error");
    };

    return self;
})();

var APIClient = (function(instance_name){
    var self = {};
    // This might fail behind a proxy. Instance name should be omitted here.
    var url_base = "/api/v1/" + instance_name + "/";

    var verify_response = function(response, callback_function){
        if( response.status == "ok" ){
            callback_function(response.data);
        } else {
            NotificationClient.error(response.message);
        }
    };

    var get_data_request = function(url, method){
        return function(callback_function){
            return send_data_request(url, method)(null, callback_function);
        }
    };

    var send_data_request = function(url, method){
        return function(json_string, callback_function){
            $.ajax({
                url: url_base + url,
                type: method,
                data: json_string,
                dataType: 'json',
                contentType: "application/json",
                success: function(response){
                    verify_response(response, callback_function);
                },
                error: function(response){
                    NotificationClient.error(JSON.parse(response.responseText));
                }
            });
        }
    };

    self.start = send_data_request("", "PUT");
    self.stop = get_data_request("", "DELETE");

    self.get_parameters = get_data_request("parameters", "GET");
    self.set_parameters = send_data_request("parameters", "POST")

    self.get_help = get_data_request("help", "GET");
    self.get_status = get_data_request("status", "GET");
    self.get_statistics = get_data_request("statistics", "GET");
    self.get_statistics_raw = get_data_request("statistics_raw", "GET")

    return self;
})(instance_name);

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
        APIClient.start(null, function(){
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