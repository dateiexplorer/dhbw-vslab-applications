const msgerForm = document.getElementById("form");
const msgerInput = document.getElementById("input");
const msgerChat = document.getElementById("chat");

console.log("executed")

var socket = null;
var uri = `ws://${window.location.host}/ws`;

window.onload = function () {
    console.log("onload")
    socket = new WebSocket(uri);

    socket.onopen = function () {
        console.log("conneted to: ", uri)
    }

    socket.onclose = function (event) {
        console.log("connection closed (" + event.code + ")");
    }

    socket.onmessage = function (event) {
        let msg = JSON.parse(event.data);
        let time = new Date(msg.timeStamp)

        let div = "";
        if (msg.clientId === msg.me) {
            div += `<div class="msg right-msg">`;
        } else {
            div += `<div class="msg left-msg">`;
        }
        
        div += `<div class="msg-bubble">
            <div class="msg-info">
                <div class="msg-info-name">${msg.sender !== "" ? msg.sender : "Unkown"}</div>
                <div class="msg-info-time">${time.getHours()}:${time.getMinutes()}</div>
            </div>
            <div class="msg-text">
                ${msg.text}
            </div>
        </div>`;

        div += "</div>";
        msgerChat.innerHTML += div; 
    }
}

msgerForm.addEventListener("submit", event => {
    event.preventDefault();

    const msgText = msgerInput.value;
    if (!msgText) return;

    let data = JSON.stringify({
        text: msgText,
    });

    socket.send(data);
});