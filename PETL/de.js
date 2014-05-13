AJS.toInit(	function() {
	initSubspacesQuickSearch()
	initSubspacesSearchCheckboxToggle();
});

function initSubspacesQuickSearch() {
 
	jQuery(".subspaces-quicksearch .subspaces-quick-search-query").each(function(){
		
		var quickSearchQuery = jQuery(this);
		
		// here we do the little placeholder stuff
		quickSearchQuery.focus(function () {
			if (jQuery(this).hasClass('placeholded')) {
				jQuery(this).val("");
				jQuery(this).removeClass("placeholded");
			}
		});
		
		quickSearchQuery.change(function () {
			if (jQuery(this).val() == "") {
				jQuery(this).val(jQuery(this).attr('placeholder'));
				jQuery(this).addClass("placeholded");
			}
		});
		
		/**
	     * function to add a tooltip showing the space name to each drop down list item
	     */
	    AJS.subspacequicksearch = AJS.quicksearch || {};
	    AJS.subspacequicksearch.dropdownPostprocess = function (list) {
	         jQuery("a span", list).each(function () {
	            var a = jQuery(this);
	            // get the hidden space name property from the span
	            var spaceName = AJS.dropDown.getAdditionalPropertyValue(a, "spaceName") || "";

	            // we need to go through html node creation so that all encoded symbols(like &gt;) are displayed correctly
	            if (spaceName) {
	                spaceName = " (" + AJS("i").html(spaceName).text() + ")";
	            }

	            a.attr("title", a.text() + spaceName);
	        });
	    };
		
		/**
		 * Append the drop down to the form element with the class quick-nav-drop-down
		 */
	    var subspacequickNavPlacement = function (input, dropDown) {
	        input.closest("form").find(".quick-nav-drop-down").append(dropDown);
	    };
		
		var subspacesSpacekey = quickSearchQuery.parent().children('.subspacesSpaceKey').val();
		var includeSubspaces = quickSearchQuery.parent().children('.includeSubspaces').val();
		
		quickSearchQuery.subspacesquicksearch("/communardo_plugins/quicksearch/subspacesQuickSearch.action"+
				"?spaceKey="+subspacesSpacekey+
				"&includeSubspaces="+includeSubspaces, null, {
			dropdownPostprocess : AJS.subspacequicksearch.dropdownPostprocess,
			dropdownPlacement : subspacequickNavPlacement
		});
	});

	
   
}


function initSubspacesSearchCheckboxToggle() {
	var topLevelSpaceCheckboxes = jQuery('#topspaces_holder .checkbox_topLevelSpaces');
	topLevelSpaceCheckboxes.click(function() {
		
		//now the checkboxes can be used like radiobuttons
		if(jQuery(this).is(':checked')) {
			topLevelSpaceCheckboxes.attr('checked', false);
			jQuery(this).attr('checked', true);
		}
		enableDisableSubspacesSearchElements();
	});
	enableDisableSubspacesSearchElements();
}

function enableDisableSubspacesSearchElements() {
	
	//disable/enable the include subspaces and spaces input element
	if(jQuery('#topspaces_holder .checkbox_topLevelSpaces').is(':checked')) {
		jQuery('#search-filter-by-space').attr("disabled", true);
		jQuery('#checkbox_include_subspaces').attr("disabled", true);
	}
	else {
		jQuery('#search-filter-by-space').attr("disabled", false);
		jQuery('#checkbox_include_subspaces').attr("disabled", false);
	}
}
(function($){
    /**
     * Options are:
     *   dropdownPostprocess - a function that will be supplied with the list created by the dropDown and can manipulate or modify it 
     *                         as necessary.
     *   dropdownPlacement - a function that will be called with the drop down and which should place it in the correct place on the page.
     *                       The supplied arguments are 1) the input that issued the search, 2) the dropDown to be placed.
     *   ajsDropDownOptions - any options the underlying dropDown component can handle expects  
     */
    $.fn.subspacesquicksearch = function(path, onShow, options) {
        options = options || {};
        var attr = {
            cache_size: 30,
            max_length: 1,
            effect: "appear"
        };
        var dd,
            cache = {},
            cache_stack = [],
            timer;

        if (typeof path == "function") {
            var oldPath = path();
            var getPath = function () {
                var newPath = path();
                if (newPath != oldPath) {
                    // reset the cache if the path has changed
                    cache = {};
                    cache_stack = [];

                    oldPath = newPath;
                }
                return newPath;
            };
        } else {
            var getPath = function () {
                return path;
            };
        }

        var searchBox = $(this);
        
        var jsonparser = function (json, resultStatus) {
            var hasErrors = json.statusMessage ? true : false; // right now, we are overloading the existence of a status message to imply an error
            var matches = hasErrors ? [[{html: json.statusMessage, className: "error"}]] : json.contentNameMatches;

            if (!hasErrors) {
                var query = json.query;
                if (!cache[query] && resultStatus == "success") {
                    cache[query] = json;
                    cache_stack.push(query);
                    if (cache_stack.length > attr.cache_size) {
                        delete cache[cache_stack.shift()];
                    }
                }
            }

            // do not update drop down for earlier requests. We are only interested in displaying the results for the most current search
            if (json.query != searchBox.val()) {
                return;
            }

            var old_dd = dd;
            
            // Standard dropDown handling of JSON object is to extract name, href, etc and then store the rest of the properties
            // as a jQuery "data" property on the name span called properties.
            dd = AJS.dropDown(matches, options.ajsDropDownOptions)[0];
            dd.jsonResult = json;
            // place the created drop down using the configured dropdownPlacement function
            // if there is none then use a default behaviour            
            if (options.dropdownPlacement) {
                options.dropdownPlacement(searchBox, dd.$);
            } else {
                searchBox.closest("form").find(".quick-nav-drop-down").append(dd.$);
            }

            dd.onhide = function (causer) {
                if (causer == "escape") {
                    searchBox.focus();
                }
            };
            var spans = $("span", dd.$);
            for (var i = 0, ii = spans.length - 1; i < ii; i++) {
                (function () {
                    var $this = $(this),
                    html = $this.html();
                    // highlight matching tokens
                    html = html.replace(new RegExp("(" + json.queryTokens.join("|") + ")", "gi"), "<strong>$1</strong>");
                    $this.html(html);
                }).call(spans[i]);
            }
            
            if (options.dropdownPostprocess) {
                options.dropdownPostprocess(dd.$);
                dd.hider = function () {
                    options.dropdownPostprocess(dd.$);
                };                
            }
            
            /**
             * Check that all items in the drop down can be displayed - show ellipses at the end of any that
             * are too long. Also remove any unused properties that the dropDown may have stored for each 
             * item in the list.
             */
            $("a span", dd.$).each(function () {
                var $a = $(this),
                    elpss = AJS("var", "&#8230;"),
                    elwidth = elpss[0].offsetWidth,
                    width = this.parentNode.parentNode.parentNode.parentNode.offsetWidth,
                    isLong = false,
                    rightPadding = 20; // add some padding so the ellipsis doesn't run over the edge of the box            
            
                AJS.dropDown.removeAllAdditionalProperties($a);
                
                $a.wrapInner($("<em>"));
                $a.append(elpss);
                this.elpss = elpss;
                $("em", $a).each(function () {
                    var $label = $(this);

                    $label.show();
                    if (this.offsetLeft + this.offsetWidth + elwidth > width - rightPadding) {

                        var childNodes = this.childNodes;
                        var success = false;

                        for (var j = childNodes.length - 1; j >= 0; j--) {
                            var childNode = childNodes[j];
                            var truncatedChars = 1;
    
                            var valueAttr = (childNode.nodeType == 3) ? "nodeValue" : "innerHTML";
                            var nodeText = childNode[valueAttr];
    
                            do {
                                if (truncatedChars <= nodeText.length) {
                                    childNode[valueAttr] = nodeText.substr(0, nodeText.length - truncatedChars++);
                                } else { // if we cannot fit even one character of the next word, then try truncating the node just previous to this
                                    break;
                                }
                            } while (this.offsetLeft + this.offsetWidth + elwidth > width - rightPadding);
    
                            if (truncatedChars <= nodeText.length) {
                                // we've managed truncate part of the word and fit it in
                                success = true;
                                break;
                            }
                        }

                        if (success) {
                            isLong = true;
                        } else {
                            $label.hide();
                        }
                    }
                });
                if (!isLong) {
                    elpss.hide();
                }
            });

            if (old_dd) {
                dd.show();
                dd.method = attr.effect;
                old_dd.$.remove();
            } else {
                dd.show(attr.effect);
            }
            if(typeof onShow == "function") {
                onShow.apply(dd);
            }            
        };
        
        searchBox.oldval = searchBox.val();
        searchBox.keyup(function (e) {
            // Don't open the search box on <enter> or <tab>
            if (e.which == 13 || e.which == 9) {
                return;
			}

            var val = searchBox.val();
            if (val != searchBox.oldval) {
                searchBox.oldval = val;

                if (!searchBox.hasClass("placeholded")) {
                    clearTimeout(timer);

                    if (AJS.params.quickNavEnabled && (/[\S]{2,}/).test(val)) {
                        if (cache[val]) {
                            jsonparser(cache[val]);
                        } else {
                        	var contextPath = jQuery('#confluence-context-path').attr('content');
                            timer = setTimeout(function () { // delay sending a request to give the user a chance to finish typing their search term(s)
                                return AJS.$.ajax({
                                    type: "GET",
                                    url: contextPath + getPath() ,
                                    data: {"query": AJS.escape(val)},
                                    success: jsonparser,
                                    dataType: "json",
                                    global: false,
                                    timeout: 5000,
                                    error: function ( xml, status, e ) { // ajax error handler
                                        if (status == "timeout") {
                                            jsonparser({statusMessage: "Timeout", query: val}, status);
                                        }
                                    }
                                });

                            }, 200);
                        }
                    } else {
                        dd && dd.hide();
                    }
                }
            }
        });

        return this;
    };
})(jQuery);

