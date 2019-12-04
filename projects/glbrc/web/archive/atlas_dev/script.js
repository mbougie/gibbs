// var map = new L.Map('map', {
//     'center': [0, 0],
//     'zoom': 0

// });


//// baselayers
var satellite = new L.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}');
var topo_map = new L.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}');
var dark_map = new L.TileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png');

//// layers


lsl_url = 'https://storage.googleapis.com/www.mattbougie.com/marginal_lsl_orange/{z}/{x}/{y}'
var lsl = new L.tileLayer(url_obj.lsl.url);

ral_url = 'https://storage.googleapis.com/www.mattbougie.com/s35_abandonment_final/{z}/{x}/{y}'
var ral = new L.tileLayer(url_obj.ral.url);

hal_url = 'https://storage.googleapis.com/www.mattbougie.com/marginal_ral_blue/{z}/{x}/{y}'
var hal = new L.tileLayer(url_obj.hal.url);



// console.log(url_obj)
// for (var key in url_obj) {
// 	console.log(key);
//     console.log(url_obj[key]);
// }



var bounds = [
    [0, -180], // Southwest coordinates
    [75, 10]  // Northeast coordinates
];

var map = L.map('map', {
	center: [36.0902, -95.7129],
	layers: [topo_map, lsl],
	zoom: 5,
	minZoom: 5,
	maxZoom: 12,
	maxBounds: bounds
});


var baseMaps = [
                 {
				    groupName : "Base Maps",
				    expanded : false,
					layers    : {
						'Satellite imagery' : satellite,
						"Reference map" : topo_map,
						"Dark map": dark_map
					}
                }							
];
		
var overlays = [
				 {
					groupName : "Marginal Land Data",
					expanded : true,
					layers    : { 
						'<span>Low capability land</span><span class = "download"><span class = "info_circle" id="lsl"></span>': lsl,
						'<span>Recently abandoned land</span><span class = "download"><span class = "info_circle" id="ral"></span>': ral,
						'<span>Formerly irrigated land</span><span class = "download"><span class = "info_circle" id="ral"></span>': hal
					}	
                 }, {
					groupName : "Irrigation",
					expanded : false,
					layers    : {}	
                 }, {
					groupName : "Carbon",
					layers    : {}	
                 }							 
];


var options = {
	container_width 	: "250px",
	group_maxHeight     : "80px",
	collapsed			: false,
	exclusive       	: true
};


// var control = L.Control.styledLayerControl(baseMaps, overlays, options);
// map.addControl(control);







////// new stuff ////////////////////////////////
var customControl = L.Control.extend({
 
  options: {
    position: 'topleft' 
    //control position - allowed: 'topleft', 'topright', 'bottomleft', 'bottomright'
  },
 
onAdd: function (map) {
    var container = L.DomUtil.create('div', 'leaflet-bar leaflet-control leaflet-control-custom yo');
    container.style.backgroundColor = 'white';
    container.style.width = '35px';
    container.style.height = '35px';
 
    container.onclick = function(){
    $('#mymodal').modal('show');
    }
    return container;
  }
 
});	

map.addControl(new customControl());







////////////////////////////////////////////////////////////
///////  create credit graphic  ////////////////////////////
////////////////////////////////////////////////////////////

// var credctrl = L.controlCredits({
//     image: "./images/logo_perc_75.jpg",
//     link: "https://www.glbrc.org/",
//     text: "GLBRC<br/>Great Lakes Bioenergy Research Center",
//     width: 254,
//     height: 112
// }).addTo(map);



////////////////////////////////////////////////////////////
///////  create legends  ///////////////////////////////////
////////////////////////////////////////////////////////////

var legend = L.control({position: 'bottomright'});


function getKey(current_url) {
	///function to get the key of the object given a value
	for (var key in url_obj) {
		if (url_obj[key].url == current_url) {
			return key
		}
	}

}





keys_array = ["lsl"]

console.log(url_obj["lsl"].hex)

legend.onAdd = function (map) {
	var div = L.DomUtil.create('div', 'info legend');
	for (var i = 0; i < keys_array.length; i++) {
		console.log(keys_array[i])
		div.innerHTML +=
			'<i style="background:' + url_obj[keys_array[i]].hex + '"></i> ' + url_obj[keys_array[i]].label + (url_obj[keys_array[i]].label ? '<br>' : '');
	}
	return div;
};

legend.addTo(map);




function addToLegend(current_key){
	console.log('rererr', current_key)
	keys_array.push(current_key);
	
	legend.onAdd = function (map) {
		var div = L.DomUtil.create('div', 'info legend')
		for (var i = 0; i < keys_array.length; i++) {
		console.log(keys_array[i])
		div.innerHTML +=
			'<i style="background:' + url_obj[keys_array[i]].hex + '"></i> ' + url_obj[keys_array[i]].label + (url_obj[keys_array[i]].label ? '<br>' : '');
	}
	return div;
	};

	legend.addTo(map);
}




function removeFromLegend(current_key){
	console.log('inside removeFromLegend', current_key)

	///remove layer from array
	var index = keys_array.indexOf(current_key);
	if (index > -1) {
	  keys_array.splice(index, 1);
	}
	
    legend.onAdd = function (map) {
		var div = L.DomUtil.create('div', 'info legend')
		for (var i = 0; i < keys_array.length; i++) {
		console.log(keys_array[i])
		div.innerHTML +=
			'<i style="background:' + url_obj[keys_array[i]].hex + '"></i> ' + url_obj[keys_array[i]].label + (url_obj[keys_array[i]].label ? '<br>' : '');
	}
	return div;
	};

	legend.addTo(map);
}

// Add this one (only) for now, as the Population layer is on by default!!!!!!!!!!!!!!!!!!!!!
// legend.addTo(map);


map.on('overlayadd', function (eventLayer) {
	console.log('overlayadd')
	console.log(eventLayer)
	current_url = eventLayer.layer._url
	current_key = getKey(current_url)
	console.log(current_key)
	addToLegend(current_key)

});


map.on('overlayremove', function (eventLayer) {
	console.log('overlayremove')
	console.log(eventLayer)
	current_url = eventLayer.layer._url
	current_key = getKey(current_url)
	console.log(current_key)
	removeFromLegend(current_key)

});



// L.control.layers(overlays,null,{collapsed:false}).addTo(map);
var control = L.Control.styledLayerControl(baseMaps, overlays, options);
map.addControl(control);
			


////// this needs to be added AFTER map.addControl(control) otherwise doesnt recognize!!

//// create a click event for the icon in layer control
$(".info_circle, .download").click(function(){
	///open modal
   $('#mymodal').modal('show');

   	///remove the label click checkbox ability so clicking on icon doesn't check the box.
   	return false; 
   
});	