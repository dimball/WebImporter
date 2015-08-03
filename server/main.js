$(document).ready(function(){
    var Tasks = {
        Order:[],
        Jobs:{},
    }
    var received = $('#received');
    var socket = new WebSocket("ws://localhost:9090/command");
    function StringToBool(value)
    {
        if (value == "True") {
            return true
        }
        else
        {
            return false
        }
    }

    function m_deSerializeTaskList(Payload)
    {
        Tasks.Order = Payload["Order"]
        console.log("deSerializing " + Payload["TaskList"].length + " Tasks")
        for (iTask in Payload["TaskList"])
        {
            var Task = Payload["TaskList"][iTask]
            if (Tasks.Jobs[Task["ID"]] == undefined)
            {
                c_Task = {
                    order:0,
                    globalorder:0,
                    TaskID:0,
                    state:"ready",
                    active:false,
                    progress:0,
                    filelist:{},
                    filelistOrder:[],
                    metadata:{},
                    type:"local"
                }

                Tasks.Jobs[Task["ID"]] = c_Task
                Tasks.Jobs[Task["ID"]].TaskID = Task["ID"]
                Tasks.Jobs[Task["ID"]].type = Task["Data"]["type"]
            }

            Tasks.Jobs[Task["ID"]]["progress"] = Task["Data"]["progress"]
            Tasks.Jobs[Task["ID"]]["metadata"] = Task["Data"]["metadata"]
            Tasks.Jobs[Task["ID"]]["filelistOrder"] = Task["Data"]["filelistOrder"]

            for (iFile in Tasks.Jobs[Task["ID"]]["filelistOrder"])
                c_file = {
                    progress:0,
                    copied:false,
                    deleting:false,
                    uploaded:false,
                    size:0
                }
                var file = Tasks.Jobs[Task["ID"]]["filelistOrder"][iFile]
                Tasks.Jobs[Task["ID"]].filelist[file] = c_file
                Tasks.Jobs[Task["ID"]].filelist[file].size = Task["Data"]["filelist"][file]["size"]
                Tasks.Jobs[Task["ID"]].filelist[file].progress = Task["Data"]["filelist"][file]["progress"]
                Tasks.Jobs[Task["ID"]].filelist[file].copied = StringToBool(Task["Data"]["filelist"][file]["progress"])
                Tasks.Jobs[Task["ID"]].filelist[file].deleting = StringToBool(Task["Data"]["filelist"][file]["delete"])
                Tasks.Jobs[Task["ID"]].filelist[file].uploaded = StringToBool(Task["Data"]["filelist"][file]["uploaded"])
            console.log("deSerialized task:" + Task["ID"])
        }
    }
    function m_create_data(command, payload)
    {
        if (typeof(payload)==='undefined') payload = 0;

        data = {}
        data["command"] = command
        data["payload"] = payload
        return JSON.stringify(data)
    }

    function UpdateList()
    {
        received.empty()
        console.log(Tasks.Order)
        for (ID in Tasks.Order) {
            console.log(Tasks.Jobs[Tasks.Order[ID]])
            if (Tasks.Jobs[Tasks.Order[ID]] != undefined)
            {
                console.log("writing to screen:" + Tasks.Jobs[Tasks.Order[ID]].TaskID)
                received.append(Tasks.Jobs[Tasks.Order[ID]].TaskID);
                received.append($('<br/>'));
            }
        };
    }

    socket.onmessage = function (message) {

        var Data = JSON.parse(message.data)
        var command = Data.command;
        var payload = Data.payload;
        console.log(payload);

        switch(command )
        {
            case "/client/v1/local/queue/task/put":
                console.log("receiving: " + payload.TaskList.length);
                m_deSerializeTaskList(payload)
                UpdateList()
            break;
            case "/client/v1/local/queue/set_priority":
            //payload is a list of IDs
            Tasks.Order = payload
            UpdateList()
        };




    };
    socket.onopen = function(message) {

    //    send request for tasks
        console.log("Connection open")
        socket.send(m_create_data("/webimporter/v1/local/queue/request"))
    }
    socket.onclose = function(){
      console.log("disconnected");
    };

    var sendMessage = function(message) {
      console.log("sending:" + message.data);
      socket.send(message.data);
    };


    // GUI Stuff
    // send a command to the serial port
    $("#cmd_send").click(function(ev){
      ev.preventDefault();
      var cmd = $('#cmd_value').val();
      sendMessage({ 'data' : cmd});
      $('#cmd_value').val("");
    });

    $('#clear').click(function(){
      received.empty();
    });


});