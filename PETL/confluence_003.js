jQuery(function($) {
    var attachments = {
        getContextPath : function() {
            return $("#confluence-context-path").attr("content");
        },

        getFieldSetAsParams : function(fieldSet) {
            var params = {};

            fieldSet.find("input").each(function() {
                params[this.name] = this.value;
            });

            return params;
        },

        initAttachmentTable : function(attachmentsTableContainer) {
            // Prevent events from being bound twice.
            // Most important for the menus because if there are
            // two events the show and hide timer gets out of whack.
            $(".attachment-history a").unbind();
            AJS.Attachments.showOlderVersions($);

            $(".attachment-menu-bar .ajs-menu-item").unbind();
            $(".attachment-menu-bar").ajsMenu();

            var params = this.getFieldSetAsParams(attachmentsTableContainer.children("fieldset"));
            
            $("a.removeAttachmentLink", attachmentsTableContainer).each(function() {
                var removeLink = $(this);
                var row = $(removeLink.parents("tr")[0]);

                removeLink.unbind();
                removeLink.click(function() {
                    if (confirm(AJS.format(params['deleteConfirmMessage'], $(".filename", row).attr("data-filename")))) {
                        // Why don't we just let the browser go to the href location? Well, I don't think
                        // it is intuitive to click the remove link and get sent to the Attachments tab.
                        // I'd prefer to stay in my current location with the contents (DOM) updated.

                        attachmentsTableContainer.fadeOut("normal", function() {

                            $("table.tableview.attachments", attachmentsTableContainer).remove();

                            var waitIcon = $(document.createElement("img"));
                            waitIcon.attr("src", attachments.getContextPath() + "/images/icons/wait.gif");
                            // Center the image
                            waitIcon.css({
                                "margin-left" : "auto",
                                "margin-right" : "auto",
                                "display" : "block"
                            }).appendTo(attachmentsTableContainer);

                            attachmentsTableContainer.show(); // We need to show the container so that the user can see the spinner.

                            AJS.safe.ajax({
                                cache: false,
                                data: {
                                    decorator: "none"
                                },
                                dataType : "html",
                                url: removeLink.attr("href"),
                                success : function() {
                                    attachments.refreshOtherAttachmentsMacroInstances(params["pageId"]);
                                }
                            });
                        });
                    }

                    return false;
                });
            });
        },

        markFileCommentFieldModified : function(theCommentField)
        {
            if (theCommentField.hasClass("blank-search")) {
                theCommentField.removeClass("blank-search");
                theCommentField.val("");
            }
        },

        initAttachmentCommentTextFields : function(uploadForm) {
            uploadForm.find("input[name^='comment_']").each(function() {
                $(this).focus(function() {
                    attachments.markFileCommentFieldModified($(this));
                });
            });
        },

        // HACK ALERT!! There's a on-body-load function injected by Confluence that focuses on the first text fields it finds in a page (CONF-14936).
        // Of course, we don't want that.
        // To work around it, the upload form will be named "inlinecommentform". Input fields of forms with that name dont't get autofocused.
        renameForms : function() {
            var oldPlaceFocusFunction = self["placeFocus"];
            if (oldPlaceFocusFunction) {
                self["placeFocus"] = function() {
                    var myPluginForms = $("div.plugin_attachments_upload_container form");
                    myPluginForms.attr("name", "inlinecommentform");
                    oldPlaceFocusFunction();
                    myPluginForms.removeAttr("name"); 
                };
            }
        },

        refreshOtherAttachmentsMacroInstances : function(contentId, successCallback, errorCallback) {
            $("div.plugin_attachments_table_container > fieldset").each(function() {
                var otherAttachmentInstanceFieldset = $(this);
                var otherAttachmentInstancePageId = $("input[name='pageId']", otherAttachmentInstanceFieldset).val();

                if (otherAttachmentInstancePageId == contentId) {
                    var clonedOtherAttachmentInstanceFieldset = $(this).clone();
                    $("input", clonedOtherAttachmentInstanceFieldset).each(function() {
                        if (!$(this).hasClass("plugin_attachments_macro_render_param"))
                            $(this).remove(); // So we don't end up sending extra params
                    });

                    $.ajax({
                        cache: false,
                        type : "GET",
                        url : attachments.getContextPath() + "/pages/plugins/attachments/rendermacro.action",
                        data: attachments.getFieldSetAsParams(clonedOtherAttachmentInstanceFieldset),
                        success : function(data) {
                            var attachmentsTableContainer = otherAttachmentInstanceFieldset.parent();
                            var attachmentsTableHtml = $(data).find("div.plugin_attachments_table_container").html();

                            attachmentsTableContainer.fadeOut("normal", function() {
                                attachmentsTableContainer.html(attachmentsTableHtml);
                                attachments.initAttachmentTable(attachmentsTableContainer);
                            });


                            attachmentsTableContainer.fadeIn("normal");
                            if (successCallback)
                                successCallback();
                        },
                        error : function() {
                            if (errorCallback)
                                errorCallback();
                        }
                    });
                }
            });
        },

        initUploadForm : function(uploadForm) {
            this.initAttachmentCommentTextFields(uploadForm);

            var uploadIframe = uploadForm.children("iframe.plugin_attachments_uploadiframe");
            var submitButton = uploadForm.find("input[name='confirm']");

            submitButton.after("<img src='" + attachments.getContextPath() + "/images/icons/wait.gif' class='plugin_attachments_uploadwaiticon hidden'/>");
            
            var waitIcon = submitButton.next("img.plugin_attachments_uploadwaiticon");
            var formParams = attachments.getFieldSetAsParams(uploadForm.parent().prev("div.plugin_attachments_table_container").children("fieldset"));

            uploadForm.submit(function() {
                if (formParams["outputType"] == "preview")
                    return false;

                // Clear out comment hints in the fields that have not been modified.
                uploadForm.find("input[name^='comment_']").each(function() {
                    attachments.markFileCommentFieldModified($(this));
                });

                var uploadFormElement = this;
                uploadFormElement.target = uploadIframe.attr("name");

                submitButton.addClass("hidden");
                waitIcon.removeClass("hidden");

                uploadIframe.get(0).processUpload = true;
                return true;
            });

            uploadIframe.load(function() {
                if (!this.processUpload) {
                    return;
                }

                var iframeDocument = this.contentWindow || this.contentDocument;
                iframeDocument = iframeDocument.document ? iframeDocument.document : iframeDocument;

                var iframeBodyElement = iframeDocument.body;
                var iframeBody = $(iframeBodyElement);
                var errorBoxInSomeHtml = iframeBody.find("div.errorBox");
                var uploadFormParent = uploadForm.parent();
                var formErrorBox = uploadFormParent.children("div.errorBox");
                var successfulMessage = uploadFormParent.children("div.successBox");

                if (errorBoxInSomeHtml.length > 0 && $.trim(errorBoxInSomeHtml.html()).length > 0) {
                    formErrorBox.html(errorBoxInSomeHtml.html());
                    formErrorBox.removeClass("hidden");
                    successfulMessage.addClass("hidden");
                    waitIcon.addClass("hidden");
                    submitButton.removeClass("hidden");
                } else {
                    attachments.refreshOtherAttachmentsMacroInstances(
                            formParams["pageId"],
                            function() {
                                formErrorBox.addClass("hidden");
                                successfulMessage.removeClass("hidden");
                                waitIcon.addClass("hidden");
                                submitButton.removeClass("hidden");
                            },
                            function() {
                                formErrorBox.html(formParams["i18n-notpermitted"]);
                                formErrorBox.removeClass("hidden");
                                successfulMessage.addClass("hidden");

                                waitIcon.addClass("hidden");
                                submitButton.removeClass("hidden");
                            }
                    );
                }
            });
        }
    };

    $("div.plugin_attachments_table_container").each(function() {
        attachments.initAttachmentTable($(this));
    });

    $("form.plugin_attachments_uploadform").each(function() {
        attachments.initUploadForm($(this));
    });

    attachments.renameForms();
});
