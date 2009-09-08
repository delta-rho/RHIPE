;;; (load "/Users/yanger/Library/Preferences/Aquamacs Emacs/customizations.el")
;;; (load "/Users/yanger/Library/Preferences/Aquamacs Emacs/Preferences.el") 
(setq org-publish-project-alist
      '(
	("rhipe-notes"
	    :base-directory "."
	    :base-extension "org"
	    :publishing-directory "website/"
	    :recursive t
	    :publishing-function org-publish-org-to-html
	    :headline-levels 4             ; Just the default for this project.
	    :auto-preamble t
	    )
	 ("rhipe" :components ("rhipe-notes" ))
      ))

(org-publish (assoc "rhipe" org-publish-project-alist))
(kill-emacs)
