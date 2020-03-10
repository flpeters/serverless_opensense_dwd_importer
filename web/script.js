//called when page is loaded
$(document).ready(function() {

    // create initial empty chart
    var approxhourly = []
    var ctx_live = document.getElementById("barChart");
    var myChart = new Chart(ctx_live, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                    data: [],
                    borderColor: "#8e5ea2",
                    label: 'aimedValueCount',
                },
                {
                    data: [],
                    borderWidth: 1,
                    borderColor: '#00c0ef',
                    label: 'valueCount',
                }
            ]
        },
        options: {
            responsive: true,
            title: {
                display: true,
                text: "Value performance",
            },
            legend: {
                display: true
            },
            scales: {
                yAxes: [{
                    ticks: {
                        beginAtZero: true,
                    },
                    scaleLabel: {
                        display: true,
                        labelString: 'Value Amount',
                        fontSize: 20
                    }
                }]
            }
        }
    });


    // current value
    var valueIter = 1;


    function getLogs() {
        /* time triggered function
         * calls server and formats logs to then write them in to the chart
         */
        if ($("#pauseLogs:checked").val() !== "on") {
            $.ajax({
                url: "http://localhost:5000/logs/",
                type: 'GET',
                contentType: "application/json",
                success: function(data) {

                    myChart.data.labels = []
                    myChart.data.datasets[0].data = []
                    myChart.data.datasets[1].data = []
                    let jsonAsObj = JSON.parse((JSON.stringify(data)))
                    let latestLogs = jsonAsObj.latestLogs
                    let logState = jsonAsObj.logState
                    if (logState === "NOT_LOGGING") {
                        $("#loggerState").html("Currently the Monitor is not logging. Is handleconfigaction deployed ?")
                    } else {
                        $("#loggerState").html("Currently the Monitor is logging.")
                    }
                    let aimedValsData = []
                    if (valueIter >= 10) {
                        valueIter -= 9
                    }
                    latestLogs.forEach(item => myChart.data.labels.push("Value " + valueIter++));

                    latestLogs.forEach(item => aimedValsData.push(item.aimedValues))
                    let valueCountData = []
                    latestLogs.forEach(item => valueCountData.push(item.reachedValues))
                    aimedValsData.forEach(item => myChart.data.datasets[0].data.push(item))
                    valueCountData.forEach(item => myChart.data.datasets[1].data.push(item))
                    if (latestLogs.length > 1) {
                        $("#reachedValueCount").html(latestLogs[latestLogs.length - 1].reachedValues - latestLogs[latestLogs.length - 2].reachedValues)
                        $("#aimedValueCount").html(latestLogs[latestLogs.length - 1].aimedValues - latestLogs[latestLogs.length - 2].aimedValues)
                        approxhourly.push((latestLogs[latestLogs.length - 1].reachedValues - latestLogs[latestLogs.length - 2].reachedValues))
                        $("#actionCount").html(latestLogs[latestLogs.length - 1].actionCount)
                    } else if (latestLogs.length === 1) {
                        $("#reachedValueCount").html(latestLogs[latestLogs.length - 1].reachedValues)
                        $("#aimedValueCount").html(latestLogs[latestLogs.length - 1].aimedValues)
                        $("#actionCount").html(latestLogs[latestLogs.length - 1].actionCount)
                        approxhourly.push(-1)
                    } else {
                        $("#reachedValueCount").html("0")
                        $("#aimedValueCount").html("0")
                        $("#actionCount").html("0")
                        approxhourly.push(-1)
                    }

                    const sum = approxhourly.filter(function(value, index, arr) { return value > -1; }).reduce((a, b) => a + b, 0);
                    const avg = (sum / approxhourly.length) || 0;
                    $("#hourlyValues").html(avg * 4 * 60)

                    // re-render the chart
                    myChart.update();
                    $("#monitorLogs").html("Monitor connected")
                }
            }).fail(function(jqXHR, textStatus, errorThrown) {
                $("#monitorLogs").html("Cant reach server.")
            });
        } else {
            $("#monitorLogs").html("Monitor Paused")
        }
    }

    // run data getter functions on init
    getLogs()
    getActions()
    importState()

    // get new logs every 15 seconds
    setInterval(getLogs, 15000);

    // get current import state and actionlist every 5 seconds
    setInterval(getActions, 5000);
    setInterval(importState, 5000)

    // load start_time from local storage
    $("#startTime").html(localStorage.getItem('start_time'))
});


//https://stackoverflow.com/questions/926332/how-to-get-formatted-date-time-like-2009-05-29-215557-using-javascript
function getFormattedDate() {
    var date = new Date();
    var str = date.getFullYear() + "-" + (date.getMonth() + 1) + "-" + date.getDate() + " " + date.getHours() + ":" + date.getMinutes() + ":" + date.getSeconds();

    return str;
}


function deployActions() {
    /* on click function for the deploy actions button
     * reads if relevant checkboxes are checked and then calls server with this information
     * to deploy the actions configured in user config
     */
    $("#deployActionsBtn").html("DEPLOYING ACTIONS !!!")
    $("#deployActionsBtn").removeClass("btn-warning")
    $("#deployActionsBtn").addClass("btn-danger")
    $.ajax({
        url: "http://localhost:5000/deploy/",
        type: 'GET',
        contentType: "application/json",
        data: {
            deployment: $("input[name=Deployment]:checked").val(),
            fresh: $("#checkBox:checked").val() === "on"
        },
        success: function(data) {
            let jsonAsObj = JSON.parse((JSON.stringify(data)))
            $("#deployActionsBtn").html("Deploy Actions")
            $("#deployActionsBtn").addClass("btn-warning")
            $("#deployActionsBtn").removeClass("btn-danger")
        }
    });
}


function deleteActions() {
    /* on click function for the delete actions button
     * reads if relevant checkboxes are checked and then calls server with this information
     * to delete all actions in a given deployment <IBM|REMOTE|LOCAL>
     */
    $.ajax({
        url: "http://localhost:5000/deleteActions/",
        type: 'GET',
        contentType: "application/json",
        data: { deployment: $("input[name=Deployment]:checked").val() },
        success: function(data) {

        }
    });
}


function importState() {
    /* time triggered function
     * reads if relevant checkboxes are checked and then calls server with this information
     * to know if any ongoing import process exists (just an approximation)
     */
    if ($("#pauseLogs:checked").val() !== "on") {
        $.ajax({
            url: "http://localhost:5000/isImporting/",
            type: 'GET',
            contentType: "application/json",
            data: { deployment: $("input[name=Deployment]:checked").val() },
            success: function(data) {
                console.log()
                $("#actionExpected").html(localStorage.getItem('actionExpected'))
                let jsonAsObj = JSON.parse((JSON.stringify(data)))
                if (jsonAsObj.message === "NO_IMPORT") {
                    $("#importData").html("Import Data")
                    $("#importData").removeClass("btn-success")
                    $("#importData").addClass("btn-warning")
                } else {
                    $("#importData").html("Importing Data!!!")
                    $("#importData").removeClass("btn-warning")
                    $("#importData").addClass("btn-success")
                }
            }
        });
    } else {
        $("#isImporting").html("paused updates")
    }
}


function clearLogs() {
    /* on click function for the clear logs button
     * calls server on clearLogs endpoint
     * to clear the logs created by the server locally
     */
    $.ajax({
        url: "http://localhost:5000/clearLogs/",
        type: 'GET',
        contentType: "application/json",
        success: function(data) {}
    });
}


function importData() {
    /* on click function for the import data button
     * reads if relevant checkboxes are checked and then calls server with this information
     * to start an import process with an given scale
     */
    $.ajax({
        url: "http://localhost:5000/import/",
        type: 'GET',
        contentType: "application/json",
        data: { calls: $("#importCalls").val() },
        success: function(data) {
            let jsonAsObj = JSON.parse((JSON.stringify(data)))
            $("#actionExpected").html(jsonAsObj.actionsExpected)
            localStorage.setItem('actionExpected', jsonAsObj.actionsExpected);
            localStorage.setItem('start_time', getFormattedDate())
            $("#startTime").html(localStorage.getItem('start_time'))
        }
    });
}


function clearMongo() {
    /* on click function for the clear mongo button
     * calls server on clearMongo endpoint
     * to clear all the serverMappings in the MongoDB
     */
    $.ajax({
        url: "http://localhost:5000/clearMongo/",
        type: 'GET',
        contentType: "application/json",
        success: function(data) {

        }
    });
}


function getActions() {
    /* time triggered function
     * calls the server on /getActions/ endpoint when pauseLogs checkbox is not checked
     * to get the list of actions in html <li> tags, which are handled server side
     */
    if ($("#pauseLogs:checked").val() !== "on") {
        $.ajax({
            url: "http://localhost:5000/getActions/",
            type: 'GET',
            contentType: "application/json",
            timeout: 15000,
            success: function(data) {
                let jsonAsObj = JSON.parse((JSON.stringify(data)))
                $("#actionList").html(jsonAsObj.message)
                $("#actionListLogs").html("ActionList available")
            }
        }).fail(function(jqXHR, textStatus, errorThrown) {
            $("#actionListLogs").html("Cant reach server")
        });
    } else {
        $("#actionListLogs").html("ActionList updates paused")
    }
}