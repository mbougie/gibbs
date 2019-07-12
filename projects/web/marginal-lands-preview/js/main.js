
//// baselayers
var satellite = new L.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}');
var topo_map = new L.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}');
var dark_map = new L.TileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png');

//// layers
var lsl = new L.tileLayer('https://storage.googleapis.com/www.mattbougie.com/marginal_lsl_orange/{z}/{x}/{y}');
var ral = new L.tileLayer('https://storage.googleapis.com/www.mattbougie.com/s35_abandonment_final/{z}/{x}/{y}');
var hal = new L.tileLayer('https://storage.googleapis.com/www.mattbougie.com/marginal_ral_blue/{z}/{x}/{y}');

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
						"Satellite imagery" : satellite,
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
						"Low suitability land": lsl,
						"Recently abandoned land": ral,
						"Historically abandoned land": hal
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
	container_width 	: "300px",
	group_maxHeight     : "80px",
	collapsed:false,
	exclusive       	: true
};

// L.control.layers(overlays,null,{collapsed:false}).addTo(map);
var control = L.Control.styledLayerControl(baseMaps, overlays, options);
map.addControl(control);
			

		
 