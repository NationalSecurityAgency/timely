# Maintaining the Documentation

This documentation is created using [Slate] (https://github.com/lord/slate). To build you will need to install some Ruby dependencies described [here] (https://github.com/lord/slate/wiki/Installing-Slate). Once you have the dependencies, then you will want to checkout the slate branch to modify the documentation.

## Viewing your changes locally

From the slate branch, execute `bundle exec middleman server` and go to http://localhost:4567 in your browser

<aside class="success">
Reload your browser to see saved changes.
</aside>

## Pushing changes to gh-pages branch

From the slate branch, execute `bundle exec middleman build --clean` which will generate the static pages in a directory called `build`. Move the build directory somewhere and checkout the gh-pages branch. Copy the contents of the build directory into the gh-pages branch, commit, and push.

## Updating Slate

The slate branch is a point-in-time copy of the master branch at https://github.com/lord/slate. You can update our slate branch to get new features using these [instructions] (https://github.com/lord/slate/wiki/Updating-Slate)

