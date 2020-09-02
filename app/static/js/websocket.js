if ("WebSocket" in window) {
} else {
    alert("data interface not supported, sorry !:(");
}

function getTaggedElement(root, use_id) {
    elementId = `${root}-${use_id}`;
    console.log(`retrieving ${elementId}`);
    return document.getElementById(elementId);
}

function newWebSocket() {
    var hostname = document.location.hostname;
    if (["127.0.0.1.", "localhost"].includes(hostname)) {
        var websocket_address = `ws://${hostname}:8080/data`;
    } else {
        var websocket_address = `wss://${hostname}/data`;
    }
    var websocket = new WebSocket(websocket_address);
    console.log(`new websocket ${websocket_address}`);
    return websocket
}

function makeId(length) {
   var result           = '';
   var characters       = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
   var charactersLength = characters.length;
   for ( var i = 0; i < length; i++ ) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
   }
   return result;
}

function drawImageGridFromWebsocket(action, values_from, img_target) {
    document.getElementById("spinner").style.display = "inline-block";
    var request = new Object();
    request.action = "get";
    request.single_value = false;
    experiment_name = document.getElementById("experiment-query").value.trim();
    if (experiment_name == "*") {
      request.experiment = null;
    } else {
      request.experiment = experiment_name;
    }
    input_element = document.getElementById("get-query");
    request.get = input_element.value.trim();
    input_element.value = "";
    
    var websocket = newWebSocket();

    websocket.onopen = function() {
        console.log(`sending webhook for grid query: ${JSON.stringify(request)}`);
        websocket.send(JSON.stringify(request));
        
    };

    websocket.onmessage = function(mesg) {
        var data = JSON.parse(mesg.data);
        console.log(`websocket responded with ${JSON.stringify(data)}`)
        document.getElementById("spinner").style.display = "none";
        switch (data.status) {
            case 200:
                var gallery = document.getElementById("gallery-query");
                var column = document.createElement("div");
                column.className = "column";
                gallery.appendChild(column);
                var i = 0;
                var j = 0;

                function appendResult(item, index) {
                    ++i
                    if (i > 5) {
                        ++j;
                        i = 0;
                        column = document.createElement("div");
                        column.className = "column";
                        gallery.appendChild(column);
                        console.log("overflow column")
                    }
                    var itemId = makeId(5);

                    var result = document.createElement("div");

                    var colorgram = document.createElement("div");
                    colorgram.className = "colorgram";
                    var img = document.createElement("img");
                    img.id = `colorgram-${itemId}`;
                    img.src = "data:image/png;base64," + item.image_bytes;
                    colorgram.appendChild(img);

                    var description = document.createElement("div");
                    description.className = "centered-text";
                    description.id = `details-${itemId}`;
                    var experiment_name = item["doc"]["_source"]["experiment_name"];
                    var query = item["doc"]["_source"]["query"];
                    var timestamp = item["doc"]["_source"]["trial_timestamp"];
                    description.innerHTML = `${query}<br>${experiment_name}<br>${timestamp}`;

                    colorgram.addEventListener("mouseover", function(event){
                            document.getElementById(`details-${itemId}`).style.display = "block";
                        }
                    );
                    colorgram.addEventListener("mouseout", function(event){
                            document.getElementById(`details-${itemId}`).style.display = "none";
                        }
                    );

                    colorgram.appendChild(description);

                    result.appendChild(colorgram);
                   
                    column.appendChild(result);
                }

                var data = JSON.parse(mesg.data)
                data.found.forEach(appendResult);
                document.getElementById("search-status").innerHTML = "200";
                break;
            default:
                document.getElementById("search-status").innerHTML = JSON.stringify(data);
        }
    };
    websocket.onclose = function() {
        console.log("websocket closing.");
    };
}

function getImageFromFormWebsocket(action, values_from, img_target, use_id) {
    var request = new Object();

    request.action = action;

    function addToRequest(item, index) {
        request[item] = getTaggedElement(item, use_id).value.trim();
    }

    values_from.forEach(addToRequest);

    var websocket = newWebSocket();

    websocket.onopen = function() {
        console.log(`sending webhook from form ${use_id}: ${JSON.stringify(request)}`);
        websocket.send(JSON.stringify(request));
    };
    websocket.onmessage = function(mesg) {
        var data = JSON.parse(mesg.data);
        console.log(`websocket responded with ${JSON.stringify(data)}`)
        var img = getTaggedElement(img_target, use_id);
        switch (data.status) {
            case 200:
                img.src = "data:image/png;base64," + data.found.image_bytes;
                hideForm("selector", use_id)
                break;
            default:
                img.src = "https://www.publicdomainpictures.net/pictures/280000/velka/not-found-image-15383864787lu.jpg";
        }
        return img.src
    };
    websocket.onclose = function() {
        console.log("websocket closing.");
    };
}

function bindSubmitButton(submitButton) {
    submitButton.addEventListener("click touchstart", 
        function(){submitButton.value=submitButton.value.toLowerCase()}
    )
    submitButton.addEventListener("click touchstart", 
        getImageFromFormWebsocket(
            action="get", 
            values_from=["get", "experiment"], 
            img_target="colorgram",
            use_id=submitButton.id.replace("submit-", "")
        )
    )
}

var buttonElements = document.getElementsByClassName("ws-submit");
var i;
for (i = 0; i < buttonElements.length; i++) {
  bindSubmitButton(buttonElements[i].style.backgroundColor);
}
