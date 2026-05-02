# Warning policy for OnTrade C++ targets. Hot-path code must compile clean.
# Apply to a target with: ontrade_apply_warnings(my_target)

function(ontrade_apply_warnings target)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
        if(ONTRADE_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    else()
        target_compile_options(${target} PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wshadow
            -Wnon-virtual-dtor
            -Woverloaded-virtual
            -Wold-style-cast
            -Wdouble-promotion
            -Wformat=2
            -Wmisleading-indentation
            -Wnull-dereference
        )
        if(ONTRADE_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()
